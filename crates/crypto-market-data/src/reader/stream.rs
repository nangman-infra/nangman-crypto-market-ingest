use super::kind::ensure_partial_depth_kind;
use super::payload::websocket_text_payload;
use crate::clock::now_ms;
use crate::error::MarketDataError;
use crate::normalize::{normalize_binance_stream_message, normalize_binance_ticker_message};
use crate::stats::MarketStreamStats;
use crate::stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig, BinanceStreamKind};
use crate::tls::install_rustls_crypto_provider;
use crypto_domain::{MarketSnapshot, TraceId};
use futures_util::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::connect_async;

pub async fn stream_binance_ticker_snapshots(
    config: &BinanceStreamConfig,
    duration: Duration,
    first_decision_trace_id: TraceId,
    sender: mpsc::Sender<MarketSnapshot>,
) -> Result<MarketStreamStats, MarketDataError> {
    install_rustls_crypto_provider();

    let url = config.combined_stream_url(BinanceStreamKind::Ticker)?;
    let (mut websocket, _) = connect_async(&url).await?;
    let deadline = Instant::now() + duration;
    let mut stats = MarketStreamStats::default();
    let mut decision_trace_id = first_decision_trace_id;

    loop {
        let Some(message) = timeout_at(deadline, websocket.next()).await.ok().flatten() else {
            break;
        };
        let message = message?;
        let Some(raw_payload) = websocket_text_payload(message)? else {
            continue;
        };
        stats.received_messages += 1;
        let snapshot =
            normalize_binance_ticker_message(config, &raw_payload, now_ms()?, decision_trace_id)?;
        decision_trace_id += 1;
        match sender.try_send(snapshot) {
            Ok(()) => stats.sent_snapshots += 1,
            Err(mpsc::error::TrySendError::Full(_)) => stats.overflowed_snapshots += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
    }

    Ok(stats)
}

pub async fn stream_binance_ticker_and_partial_depth_snapshots(
    config: &BinanceStreamConfig,
    duration: Duration,
    depth_kind: BinanceStreamKind,
    first_decision_trace_id: TraceId,
    sender: mpsc::Sender<BinanceNormalizedMarketEvent>,
) -> Result<MarketStreamStats, MarketDataError> {
    ensure_partial_depth_kind(
        depth_kind,
        "public replay recorder requires a partial depth stream kind",
    )?;

    install_rustls_crypto_provider();

    let url = config.combined_stream_url_for_kinds(&[BinanceStreamKind::Ticker, depth_kind])?;
    let (mut websocket, _) = connect_async(&url).await?;
    let deadline = Instant::now() + duration;
    let mut stats = MarketStreamStats::default();
    let mut decision_trace_id = first_decision_trace_id;

    loop {
        let Some(message) = timeout_at(deadline, websocket.next()).await.ok().flatten() else {
            break;
        };
        let message = message?;
        let Some(raw_payload) = websocket_text_payload(message)? else {
            continue;
        };
        stats.received_messages += 1;
        let event =
            normalize_binance_stream_message(config, &raw_payload, now_ms()?, decision_trace_id)?;
        decision_trace_id += 1;
        match sender.try_send(event) {
            Ok(()) => stats.sent_snapshots += 1,
            Err(mpsc::error::TrySendError::Full(_)) => stats.overflowed_snapshots += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
    }

    Ok(stats)
}
