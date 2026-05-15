use crate::clock::now_ms;
use crate::depth_sync::{BinanceDepthSyncSettings, BinanceLocalOrderBook, handle_diff_depth_event};
use crate::error::MarketDataError;
use crate::messages::BinanceDiffDepthMessage;
use crate::normalize::{
    normalize_binance_stream_message, sequence_from_binance_payload, stream_kind_from_stream,
    stream_symbol, unwrap_binance_payload,
};
use crate::stats::BinanceIngestWatchStats;
use crate::stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig, BinanceStreamKind};
use crate::tls::install_rustls_crypto_provider;
use crypto_domain::{Sequence, TimestampMs, TraceId};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::str;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{connect_async, tungstenite};

pub async fn watch_binance_ingest_streams<F>(
    config: &BinanceStreamConfig,
    duration: Duration,
    kinds: &[BinanceStreamKind],
    log_interval: Duration,
    first_decision_trace_id: TraceId,
    mut on_log: F,
) -> Result<BinanceIngestWatchStats, MarketDataError>
where
    F: FnMut(&BinanceIngestWatchStats),
{
    install_rustls_crypto_provider();

    let stream_names = config.combined_stream_names_for_kinds(kinds)?;
    let url = config.combined_stream_url_for_kinds(kinds)?;
    let (mut websocket, _) = connect_async(&url).await?;
    let deadline = Instant::now() + duration;
    let mut next_log = Instant::now() + log_interval;
    let mut stats = BinanceIngestWatchStats::new(url, stream_names.len());
    let mut last_sequence_by_stream = BTreeMap::<String, Sequence>::new();
    let mut decision_trace_id = first_decision_trace_id;

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let wait_until = next_log.min(deadline);
        match timeout_at(wait_until, websocket.next()).await {
            Ok(Some(message)) => {
                let message = message?;
                match classify_websocket_message(message, &mut stats, false)? {
                    IngestWebSocketMessage::Payload(text) => {
                        observe_binance_ingest_payload(
                            config,
                            &text,
                            now_ms()?,
                            decision_trace_id,
                            &mut last_sequence_by_stream,
                            &mut stats,
                        );
                        decision_trace_id += 1;
                    }
                    IngestWebSocketMessage::Pong(payload) => {
                        websocket.send(tungstenite::Message::Pong(payload)).await?;
                    }
                    IngestWebSocketMessage::Closed => break,
                    IngestWebSocketMessage::Control => {}
                }
                advance_log_if_due(&mut next_log, log_interval, || on_log(&stats));
            }
            Ok(None) => break,
            Err(_) => {
                advance_log_if_due(&mut next_log, log_interval, || on_log(&stats));
            }
        }
    }

    on_log(&stats);
    Ok(stats)
}

pub async fn watch_binance_ingest_streams_with_depth_sync<F>(
    config: &BinanceStreamConfig,
    depth_sync: &BinanceDepthSyncSettings,
    duration: Duration,
    kinds: &[BinanceStreamKind],
    log_interval: Duration,
    first_decision_trace_id: TraceId,
    mut on_log: F,
) -> Result<BinanceIngestWatchStats, MarketDataError>
where
    F: FnMut(&BinanceIngestWatchStats),
{
    install_rustls_crypto_provider();

    let stream_names = config.combined_stream_names_for_kinds(kinds)?;
    let url = config.combined_stream_url_for_kinds(kinds)?;
    let (mut websocket, _) = connect_async(&url).await?;
    let http_client = reqwest::Client::new();
    let deadline = Instant::now() + duration;
    let mut next_log = Instant::now() + log_interval;
    let mut stats = BinanceIngestWatchStats::new(url, stream_names.len());
    let mut last_sequence_by_stream = BTreeMap::<String, Sequence>::new();
    let mut books = BTreeMap::<String, BinanceLocalOrderBook>::new();
    let mut snapshot_attempted = HashSet::<String>::new();
    let mut decision_trace_id = first_decision_trace_id;

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let wait_until = next_log.min(deadline);
        match timeout_at(wait_until, websocket.next()).await {
            Ok(Some(message)) => {
                let message = message?;
                match classify_websocket_message(message, &mut stats, true)? {
                    IngestWebSocketMessage::Payload(text) => {
                        observe_binance_ingest_payload_with_depth_sync(
                            config,
                            depth_sync,
                            &http_client,
                            &text,
                            now_ms()?,
                            decision_trace_id,
                            DepthObserveState {
                                last_sequence_by_stream: &mut last_sequence_by_stream,
                                books: &mut books,
                                snapshot_attempted: &mut snapshot_attempted,
                                stats: &mut stats,
                            },
                        )
                        .await?;
                        decision_trace_id += 1;
                    }
                    IngestWebSocketMessage::Pong(payload) => {
                        websocket.send(tungstenite::Message::Pong(payload)).await?;
                    }
                    IngestWebSocketMessage::Closed => {
                        break;
                    }
                    IngestWebSocketMessage::Control => {}
                }
                stats.update_depth_book_counts(&books);
                advance_log_if_due(&mut next_log, log_interval, || {
                    on_log(&stats);
                });
            }
            Ok(None) => {
                stats.source_health_status = "ended".to_owned();
                stats.source_health_events += 1;
                break;
            }
            Err(_) => {
                stats.source_health_status = "waiting_for_messages".to_owned();
                stats.source_health_events += 1;
                advance_log_if_due(&mut next_log, log_interval, || {
                    stats.update_depth_book_counts(&books);
                    on_log(&stats);
                });
            }
        }
    }

    stats.update_depth_book_counts(&books);
    on_log(&stats);
    Ok(stats)
}

enum IngestWebSocketMessage {
    Payload(String),
    Pong(tungstenite::Bytes),
    Closed,
    Control,
}

fn classify_websocket_message(
    message: tungstenite::Message,
    stats: &mut BinanceIngestWatchStats,
    track_source_health: bool,
) -> Result<IngestWebSocketMessage, MarketDataError> {
    match message {
        tungstenite::Message::Text(text) => Ok(IngestWebSocketMessage::Payload(text.to_string())),
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref()).map_err(|error| {
                MarketDataError::InvalidMessage(format!(
                    "binary websocket payload is not utf-8: {error}"
                ))
            })?;
            Ok(IngestWebSocketMessage::Payload(text.to_owned()))
        }
        tungstenite::Message::Ping(payload) => {
            stats.control_messages += 1;
            stats.pings_received += 1;
            if track_source_health {
                stats.source_health_status = "connected".to_owned();
            }
            Ok(IngestWebSocketMessage::Pong(payload))
        }
        tungstenite::Message::Pong(_) => {
            stats.control_messages += 1;
            stats.pongs_received += 1;
            Ok(IngestWebSocketMessage::Control)
        }
        tungstenite::Message::Close(_) => {
            stats.control_messages += 1;
            stats.close_messages += 1;
            if track_source_health {
                stats.source_health_status = "closed".to_owned();
                stats.source_health_events += 1;
            }
            Ok(IngestWebSocketMessage::Closed)
        }
        tungstenite::Message::Frame(_) => {
            stats.control_messages += 1;
            Ok(IngestWebSocketMessage::Control)
        }
    }
}

fn advance_log_if_due(next_log: &mut Instant, log_interval: Duration, mut on_due: impl FnMut()) {
    if Instant::now() < *next_log {
        return;
    }
    on_due();
    while *next_log <= Instant::now() {
        *next_log += log_interval;
    }
}

pub(crate) fn observe_binance_ingest_payload(
    config: &BinanceStreamConfig,
    raw_payload: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
    last_sequence_by_stream: &mut BTreeMap<String, Sequence>,
    stats: &mut BinanceIngestWatchStats,
) {
    stats.received_messages += 1;
    let Ok(value) = serde_json::from_str::<Value>(raw_payload) else {
        stats.malformed_messages += 1;
        return;
    };
    let Ok(payload) = unwrap_binance_payload(value) else {
        stats.malformed_messages += 1;
        return;
    };
    let Some(stream) = payload.stream.as_deref() else {
        stats.malformed_messages += 1;
        return;
    };
    let Ok(kind) = stream_kind_from_stream(stream) else {
        stats.malformed_messages += 1;
        return;
    };
    let Some(raw_symbol) = stream_symbol(stream) else {
        stats.malformed_messages += 1;
        return;
    };

    stats.parsed_messages += 1;
    stats.increment_kind(kind);
    stats.increment_symbol(raw_symbol);

    record_sequence_observation(kind, &payload.data, stream, last_sequence_by_stream, stats);

    match kind {
        BinanceStreamKind::Ticker
        | BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => {
            match normalize_binance_stream_message(
                config,
                raw_payload,
                received_time_ms,
                decision_trace_id,
            ) {
                Ok(BinanceNormalizedMarketEvent::Market(_)) => {
                    stats.normalized_market_snapshots += 1;
                }
                Ok(BinanceNormalizedMarketEvent::Depth(_)) => {
                    stats.normalized_depth_snapshots += 1;
                }
                Err(_) => {
                    stats.normalization_errors += 1;
                }
            }
        }
        BinanceStreamKind::Trade
        | BinanceStreamKind::BookTicker
        | BinanceStreamKind::DiffDepth100ms => {}
    }
}

struct DepthObserveState<'a> {
    last_sequence_by_stream: &'a mut BTreeMap<String, Sequence>,
    books: &'a mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &'a mut HashSet<String>,
    stats: &'a mut BinanceIngestWatchStats,
}

async fn observe_binance_ingest_payload_with_depth_sync(
    config: &BinanceStreamConfig,
    depth_sync: &BinanceDepthSyncSettings,
    http_client: &reqwest::Client,
    raw_payload: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
    state: DepthObserveState<'_>,
) -> Result<(), MarketDataError> {
    let DepthObserveState {
        last_sequence_by_stream,
        books,
        snapshot_attempted,
        stats,
    } = state;

    stats.received_messages += 1;
    let Ok(value) = serde_json::from_str::<Value>(raw_payload) else {
        stats.malformed_messages += 1;
        return Ok(());
    };
    let Ok(payload) = unwrap_binance_payload(value) else {
        stats.malformed_messages += 1;
        return Ok(());
    };
    let Some(stream) = payload.stream.as_deref() else {
        stats.malformed_messages += 1;
        return Ok(());
    };
    let Ok(kind) = stream_kind_from_stream(stream) else {
        stats.malformed_messages += 1;
        return Ok(());
    };
    let Some(raw_symbol) = stream_symbol(stream) else {
        stats.malformed_messages += 1;
        return Ok(());
    };

    stats.parsed_messages += 1;
    stats.increment_kind(kind);
    stats.increment_symbol(raw_symbol);

    record_sequence_observation(kind, &payload.data, stream, last_sequence_by_stream, stats);

    match kind {
        BinanceStreamKind::DiffDepth100ms => {
            stats.depth_delta_messages += 1;
            let event: BinanceDiffDepthMessage = serde_json::from_value(payload.data)?;
            handle_diff_depth_event(
                depth_sync,
                http_client,
                event,
                received_time_ms,
                books,
                snapshot_attempted,
                stats,
            )
            .await?;
        }
        BinanceStreamKind::Ticker
        | BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => {
            match normalize_binance_stream_message(
                config,
                raw_payload,
                received_time_ms,
                decision_trace_id,
            ) {
                Ok(BinanceNormalizedMarketEvent::Market(_)) => {
                    stats.normalized_market_snapshots += 1;
                }
                Ok(BinanceNormalizedMarketEvent::Depth(_)) => {
                    stats.normalized_depth_snapshots += 1;
                }
                Err(_) => {
                    stats.normalization_errors += 1;
                }
            }
        }
        BinanceStreamKind::Trade | BinanceStreamKind::BookTicker => {}
    }
    Ok(())
}

fn record_sequence_observation(
    kind: BinanceStreamKind,
    payload: &Value,
    stream: &str,
    last_sequence_by_stream: &mut BTreeMap<String, Sequence>,
    stats: &mut BinanceIngestWatchStats,
) {
    if let Some(sequence) = sequence_from_binance_payload(kind, payload)
        && last_sequence_by_stream
            .insert(stream.to_owned(), sequence)
            .is_some_and(|previous| sequence <= previous)
    {
        stats.sequence_anomalies += 1;
    }
}
