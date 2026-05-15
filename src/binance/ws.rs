use super::events::{BinanceParsedEnvelope, parse_binance_payload};
use super::stats::BinanceL0WatchStats;
use super::{BinanceIngestError, BinanceMarket};
use crate::shutdown::ShutdownListener;
use crate::storage::L0StorageSink;
use crate::storage::record::RawMarketEventDraft;
use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind};
use futures_util::{SinkExt, StreamExt};
use std::cmp;
use std::collections::HashMap;
use std::str;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{connect_async, tungstenite};

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
    let (mut websocket, _) = connect_async(&url).await?;
    let mut shutdown = ShutdownListener::new().map_err(|error| {
        BinanceIngestError::InvalidMessage(format!("failed to install shutdown listener: {error}"))
    })?;

    let deadline = Instant::now() + duration;
    let mut next_log_at = Instant::now() + log_interval;
    let mut next_ping_at = Instant::now() + Duration::from_secs(30);

    while Instant::now() < deadline {
        let next_tick = cmp::min(cmp::min(deadline, next_log_at), next_ping_at);
        tokio::select! {
            _ = shutdown.wait() => {
                stats.source_health_status = "shutdown".to_owned();
                stats.source_health_events += 1;
                break;
            }
            message = timeout_at(next_tick, websocket.next()) => {
                match message {
                    Ok(Some(Ok(message))) => {
                        if let Err(error) = handle_message(
                            message,
                            &mut stats,
                            storage.as_deref_mut(),
                            &markets_by_raw,
                        )
                        .await
                        {
                            stats.malformed_messages += 1;
                            stats.source_health_status = "message_error".to_owned();
                            stats.source_health_events += 1;
                            let _ = error;
                            break;
                        }
                    }
                    Ok(Some(Err(_error))) => {
                        stats.malformed_messages += 1;
                        stats.source_health_status = "websocket_error".to_owned();
                        stats.source_health_events += 1;
                        break;
                    }
                    Ok(None) => {
                        stats.source_health_status = "ended".to_owned();
                        stats.source_health_events += 1;
                        break;
                    }
                    Err(_) => {}
                }
            }
        }

        let now = Instant::now();
        if now >= next_ping_at {
            if websocket
                .send(tungstenite::Message::Ping(Vec::new().into()))
                .await
                .is_err()
            {
                stats.source_health_status = "ping_failed".to_owned();
                stats.source_health_events += 1;
                break;
            }
            next_ping_at += Duration::from_secs(30);
        }
        if now >= next_log_at {
            stats.update_health();
            log_callback(&stats);
            next_log_at += log_interval;
        }
    }

    stats.update_health();
    Ok(stats)
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
    let detected_at_ms = unix_timestamp_ms();
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

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
