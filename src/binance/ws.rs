use super::events::{BinanceParsedEnvelope, parse_binance_payload};
use super::stats::{BinanceL0GapAlert, BinanceL0WatchStats};
use super::{BinanceIngestError, BinanceMarket};
use crate::clock;
use crate::reconnect::ReconnectPolicy;
use crate::shutdown::ShutdownListener;
use crate::storage::L0StorageSink;
use crate::storage::record::RawMarketEventDraft;
use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind};
use futures_util::{SinkExt, StreamExt};
use std::cmp;
use std::collections::HashMap;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite};

type Websocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum SessionEnd {
    ShutdownRequested,
    DeadlineReached,
    Disconnected(&'static str),
}

pub async fn watch_binance_l0_streams(
    stream_config: &BinanceStreamConfig,
    markets: &[BinanceMarket],
    stream_kinds: &[BinanceStreamKind],
    duration: Duration,
    log_interval: Duration,
    mut storage: Option<&mut L0StorageSink>,
    log_callback: impl Fn(&BinanceL0WatchStats),
) -> Result<BinanceL0WatchStats, BinanceIngestError> {
    install_rustls_crypto_provider();

    let url = stream_config.combined_stream_url_for_kinds(stream_kinds)?;
    let planned_stream_count = stream_config
        .combined_stream_names_for_kinds(stream_kinds)?
        .len();
    let markets_by_raw = markets
        .iter()
        .map(|market| (market.raw_symbol.to_ascii_uppercase(), market.clone()))
        .collect::<HashMap<_, _>>();
    let mut stats = BinanceL0WatchStats::new(url.clone(), planned_stream_count);
    let shutdown_flag = spawn_shutdown_flag()?;

    let policy = ReconnectPolicy::default_24x7();
    let mut current_backoff = policy.initial_backoff;
    let deadline = Instant::now() + duration;

    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            stats.source_health_status = "shutdown".to_owned();
            stats.source_health_events += 1;
            break;
        }
        if Instant::now() >= deadline {
            stats.source_health_status = "deadline_reached".to_owned();
            stats.source_health_events += 1;
            break;
        }

        let websocket = match connect_async(&url).await {
            Ok((ws, _)) => ws,
            Err(_) => {
                record_reconnect(&mut stats, "connect_failed");
                sleep_or_shutdown(&shutdown_flag, current_backoff).await;
                current_backoff = policy.next_backoff(current_backoff);
                continue;
            }
        };
        // Backoff resets only after a healthy session (stats event observed).
        let session_outcome = run_session(
            websocket,
            &mut stats,
            storage.as_deref_mut(),
            &markets_by_raw,
            deadline,
            log_interval,
            policy.stale_message_timeout,
            &log_callback,
            &shutdown_flag,
        )
        .await?;

        match session_outcome {
            SessionEnd::ShutdownRequested => {
                stats.source_health_status = "shutdown".to_owned();
                stats.source_health_events += 1;
                break;
            }
            SessionEnd::DeadlineReached => {
                stats.source_health_status = "deadline_reached".to_owned();
                stats.source_health_events += 1;
                break;
            }
            SessionEnd::Disconnected(reason) => {
                record_reconnect(&mut stats, reason);
                sleep_or_shutdown(&shutdown_flag, current_backoff).await;
                current_backoff = policy.next_backoff(current_backoff);
            }
        }
    }

    stats.update_health();
    Ok(stats)
}

fn spawn_shutdown_flag() -> Result<Arc<AtomicBool>, BinanceIngestError> {
    let flag = Arc::new(AtomicBool::new(false));
    let mut listener = ShutdownListener::new().map_err(|error| {
        BinanceIngestError::InvalidMessage(format!("failed to install shutdown listener: {error}"))
    })?;
    let signal_flag = Arc::clone(&flag);
    tokio::spawn(async move {
        listener.wait().await;
        signal_flag.store(true, Ordering::SeqCst);
    });
    Ok(flag)
}

async fn sleep_or_shutdown(shutdown_flag: &Arc<AtomicBool>, duration: Duration) {
    let target = Instant::now() + duration;
    while Instant::now() < target {
        if shutdown_flag.load(Ordering::SeqCst) {
            return;
        }
        let remaining = target.saturating_duration_since(Instant::now());
        let step = cmp::min(remaining, Duration::from_millis(250));
        if step.is_zero() {
            return;
        }
        tokio::time::sleep(step).await;
    }
}

fn record_reconnect(stats: &mut BinanceL0WatchStats, reason: &'static str) {
    let now_ms = clock::now_ms();
    stats.reconnect_count += 1;
    stats.last_reconnect_at_ms = Some(now_ms);
    stats.source_health_status = "reconnecting".to_owned();
    stats.source_health_events += 1;
    stats.record_gap_alert(BinanceL0GapAlert {
        gap_type: "ws_reconnect".to_owned(),
        symbol: String::new(),
        detected_at_ms: now_ms,
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "reconnect_with_backoff".to_owned(),
        heal_status: reason.to_owned(),
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    mut websocket: Websocket,
    stats: &mut BinanceL0WatchStats,
    mut storage: Option<&mut L0StorageSink>,
    markets_by_raw: &HashMap<String, BinanceMarket>,
    deadline: Instant,
    log_interval: Duration,
    stale_timeout: Duration,
    log_callback: &impl Fn(&BinanceL0WatchStats),
    shutdown_flag: &Arc<AtomicBool>,
) -> Result<SessionEnd, BinanceIngestError> {
    let now = Instant::now();
    let mut next_log_at = now + log_interval;
    let mut next_ping_at = now + Duration::from_secs(30);
    let mut last_message_at = now;

    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            return Ok(SessionEnd::ShutdownRequested);
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(SessionEnd::DeadlineReached);
        }
        let stale_at = last_message_at + stale_timeout;
        let poll_tick = now + Duration::from_millis(250);
        let next_tick = [deadline, next_log_at, next_ping_at, stale_at, poll_tick]
            .into_iter()
            .min()
            .unwrap_or(deadline);

        match timeout_at(next_tick, websocket.next()).await {
            Ok(Some(Ok(message))) => {
                last_message_at = Instant::now();
                if let Err(error) =
                    handle_message(message, stats, storage.as_deref_mut(), markets_by_raw).await
                {
                    let _ = error;
                    return Ok(SessionEnd::Disconnected("message_error"));
                }
            }
            Ok(Some(Err(_))) => {
                stats.malformed_messages += 1;
                return Ok(SessionEnd::Disconnected("websocket_error"));
            }
            Ok(None) => {
                return Ok(SessionEnd::Disconnected("ended"));
            }
            Err(_) => {} // tick wake-up — fall through to housekeeping.
        }

        let now = Instant::now();
        if now >= last_message_at + stale_timeout {
            return Ok(SessionEnd::Disconnected("stale_timeout"));
        }
        if now >= next_ping_at {
            if websocket
                .send(tungstenite::Message::Ping(Vec::new().into()))
                .await
                .is_err()
            {
                return Ok(SessionEnd::Disconnected("ping_failed"));
            }
            next_ping_at += Duration::from_secs(30);
        }
        if now >= next_log_at {
            stats.update_health();
            log_callback(stats);
            next_log_at += log_interval;
        }
    }
}

async fn handle_message(
    message: tungstenite::Message,
    stats: &mut BinanceL0WatchStats,
    storage: Option<&mut L0StorageSink>,
    markets_by_raw: &HashMap<String, BinanceMarket>,
) -> Result<(), BinanceIngestError> {
    match message {
        tungstenite::Message::Text(text) => {
            record_text_payload(text.as_ref(), stats, storage, markets_by_raw).await
        }
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref())?;
            record_text_payload(text, stats, storage, markets_by_raw).await
        }
        tungstenite::Message::Ping(_payload) => {
            stats.control_messages += 1;
            stats.pings_received += 1;
            Ok(())
        }
        tungstenite::Message::Pong(_) => {
            stats.control_messages += 1;
            stats.pongs_received += 1;
            Ok(())
        }
        tungstenite::Message::Close(_) => {
            stats.control_messages += 1;
            stats.close_messages += 1;
            stats.source_health_status = "closed".to_owned();
            stats.source_health_events += 1;
            Ok(())
        }
        tungstenite::Message::Frame(_) => Ok(()),
    }
}

async fn record_text_payload(
    raw_json: &str,
    stats: &mut BinanceL0WatchStats,
    storage: Option<&mut L0StorageSink>,
    markets_by_raw: &HashMap<String, BinanceMarket>,
) -> Result<(), BinanceIngestError> {
    stats.received_messages += 1;
    let detected_at_ms = clock::now_ms();
    match parse_binance_payload(raw_json) {
        Ok(envelope) => {
            if let Some(sink) = storage
                && let Some(draft) =
                    raw_market_event_draft(&envelope, markets_by_raw, detected_at_ms)
            {
                sink.append_raw_market_event(draft)
                    .await
                    .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
            }
            stats.record_event(envelope, detected_at_ms);
        }
        Err(error) => {
            stats.malformed_messages += 1;
            let _ = error;
        }
    }
    Ok(())
}

fn raw_market_event_draft(
    envelope: &BinanceParsedEnvelope,
    markets_by_raw: &HashMap<String, BinanceMarket>,
    ingest_timestamp_ms: i64,
) -> Option<RawMarketEventDraft> {
    let market = markets_by_raw.get(&envelope.event.symbol().to_ascii_uppercase())?;
    Some(RawMarketEventDraft {
        event_type: envelope.event.storage_event_type().to_owned(),
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.raw_symbol.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: envelope.event.exchange_timestamp_ms(ingest_timestamp_ms),
        ingest_timestamp_ms,
        sequence_id: envelope.event.sequence_id(),
        sequence_tag: envelope.event.sequence_id(),
        exchange_sequence: envelope.event.exchange_sequence(),
        diff_first_update_id: envelope.event.diff_first_update_id(),
        diff_final_update_id: envelope.event.diff_final_update_id(),
        is_snapshot: envelope.event.storage_event_type() == "depth_snapshot",
        stream_type: "REALTIME".to_owned(),
        stream_phase: "realtime".to_owned(),
        payload_json: envelope.payload_json.clone(),
    })
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
