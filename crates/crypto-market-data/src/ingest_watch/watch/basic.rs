use super::super::message::{
    IngestWebSocketMessage, advance_log_if_due, classify_websocket_message,
};
use super::super::observe::observe_binance_ingest_payload;
use crate::clock::now_ms;
use crate::error::MarketDataError;
use crate::stats::BinanceIngestWatchStats;
use crate::stream_config::{BinanceStreamConfig, BinanceStreamKind};
use crate::tls::install_rustls_crypto_provider;
use crypto_domain::{Sequence, TraceId};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{connect_async, tungstenite};

#[deprecated(note = "use src/binance/ws.rs::watch_binance_l0_streams")]
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
