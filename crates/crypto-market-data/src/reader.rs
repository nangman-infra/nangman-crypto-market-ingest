use crate::clock::now_ms;
use crate::error::MarketDataError;
use crate::normalize::{
    normalize_binance_partial_depth_message, normalize_binance_stream_message,
    normalize_binance_ticker_message,
};
use crate::stats::MarketStreamStats;
use crate::stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig, BinanceStreamKind};
use crate::tls::install_rustls_crypto_provider;
use crypto_domain::{MarketDepthSnapshot, MarketSnapshot, TraceId};
use futures_util::StreamExt;
use std::str;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{connect_async, tungstenite};

pub async fn read_one_binance_ticker_snapshot(
    config: &BinanceStreamConfig,
    decision_trace_id: TraceId,
) -> Result<MarketSnapshot, MarketDataError> {
    install_rustls_crypto_provider();

    let url = config.combined_stream_url(BinanceStreamKind::Ticker)?;
    let (mut websocket, _) = connect_async(&url).await?;

    while let Some(message) = websocket.next().await {
        let message = message?;
        match message {
            tungstenite::Message::Text(text) => {
                return normalize_binance_ticker_message(
                    config,
                    text.as_ref(),
                    now_ms()?,
                    decision_trace_id,
                );
            }
            tungstenite::Message::Binary(bytes) => {
                let text = str::from_utf8(bytes.as_ref()).map_err(|error| {
                    MarketDataError::InvalidMessage(format!(
                        "binary websocket payload is not utf-8: {error}"
                    ))
                })?;
                return normalize_binance_ticker_message(
                    config,
                    text,
                    now_ms()?,
                    decision_trace_id,
                );
            }
            tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => continue,
            tungstenite::Message::Close(frame) => {
                return Err(MarketDataError::InvalidMessage(format!(
                    "websocket closed before ticker payload: {frame:?}"
                )));
            }
            tungstenite::Message::Frame(_) => continue,
        }
    }

    Err(MarketDataError::InvalidMessage(
        "websocket ended before ticker payload".to_owned(),
    ))
}

pub async fn read_one_binance_partial_depth_snapshot(
    config: &BinanceStreamConfig,
    kind: BinanceStreamKind,
    decision_trace_id: TraceId,
) -> Result<MarketDepthSnapshot, MarketDataError> {
    if !matches!(
        kind,
        BinanceStreamKind::PartialDepth5
            | BinanceStreamKind::PartialDepth10
            | BinanceStreamKind::PartialDepth20
    ) {
        return Err(MarketDataError::InvalidMessage(
            "partial depth reader requires a partial depth stream kind".to_owned(),
        ));
    }

    install_rustls_crypto_provider();

    let url = config.combined_stream_url(kind)?;
    let (mut websocket, _) = connect_async(&url).await?;

    while let Some(message) = websocket.next().await {
        let message = message?;
        match message {
            tungstenite::Message::Text(text) => {
                return normalize_binance_partial_depth_message(
                    config,
                    text.as_ref(),
                    now_ms()?,
                    decision_trace_id,
                );
            }
            tungstenite::Message::Binary(bytes) => {
                let text = str::from_utf8(bytes.as_ref()).map_err(|error| {
                    MarketDataError::InvalidMessage(format!(
                        "binary websocket payload is not utf-8: {error}"
                    ))
                })?;
                return normalize_binance_partial_depth_message(
                    config,
                    text,
                    now_ms()?,
                    decision_trace_id,
                );
            }
            tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => continue,
            tungstenite::Message::Close(frame) => {
                return Err(MarketDataError::InvalidMessage(format!(
                    "websocket closed before depth payload: {frame:?}"
                )));
            }
            tungstenite::Message::Frame(_) => continue,
        }
    }

    Err(MarketDataError::InvalidMessage(
        "websocket ended before depth payload".to_owned(),
    ))
}

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
    if !matches!(
        depth_kind,
        BinanceStreamKind::PartialDepth5
            | BinanceStreamKind::PartialDepth10
            | BinanceStreamKind::PartialDepth20
    ) {
        return Err(MarketDataError::InvalidMessage(
            "public replay recorder requires a partial depth stream kind".to_owned(),
        ));
    }

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

fn websocket_text_payload(
    message: tungstenite::Message,
) -> Result<Option<String>, MarketDataError> {
    match message {
        tungstenite::Message::Text(text) => Ok(Some(text.to_string())),
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref()).map_err(|error| {
                MarketDataError::InvalidMessage(format!(
                    "binary websocket payload is not utf-8: {error}"
                ))
            })?;
            Ok(Some(text.to_owned()))
        }
        tungstenite::Message::Ping(_)
        | tungstenite::Message::Pong(_)
        | tungstenite::Message::Frame(_) => Ok(None),
        tungstenite::Message::Close(frame) => Err(MarketDataError::InvalidMessage(format!(
            "websocket closed before ticker payload: {frame:?}"
        ))),
    }
}
