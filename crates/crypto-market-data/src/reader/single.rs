use super::kind::ensure_partial_depth_kind;
use super::payload::websocket_text_payload_for;
use crate::clock::now_ms;
use crate::error::MarketDataError;
use crate::normalize::{normalize_binance_partial_depth_message, normalize_binance_ticker_message};
use crate::stream_config::{BinanceStreamConfig, BinanceStreamKind};
use crate::tls::install_rustls_crypto_provider;
use crypto_domain::{MarketDepthSnapshot, MarketSnapshot, TraceId};
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;

pub async fn read_one_binance_ticker_snapshot(
    config: &BinanceStreamConfig,
    decision_trace_id: TraceId,
) -> Result<MarketSnapshot, MarketDataError> {
    install_rustls_crypto_provider();

    let url = config.combined_stream_url(BinanceStreamKind::Ticker)?;
    let (mut websocket, _) = connect_async(&url).await?;

    while let Some(message) = websocket.next().await {
        let message = message?;
        let Some(raw_payload) = websocket_text_payload_for(message, "ticker payload")? else {
            continue;
        };
        return normalize_binance_ticker_message(
            config,
            &raw_payload,
            now_ms()?,
            decision_trace_id,
        );
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
    ensure_partial_depth_kind(
        kind,
        "partial depth reader requires a partial depth stream kind",
    )?;

    install_rustls_crypto_provider();

    let url = config.combined_stream_url(kind)?;
    let (mut websocket, _) = connect_async(&url).await?;

    while let Some(message) = websocket.next().await {
        let message = message?;
        let Some(raw_payload) = websocket_text_payload_for(message, "depth payload")? else {
            continue;
        };
        return normalize_binance_partial_depth_message(
            config,
            &raw_payload,
            now_ms()?,
            decision_trace_id,
        );
    }

    Err(MarketDataError::InvalidMessage(
        "websocket ended before depth payload".to_owned(),
    ))
}
