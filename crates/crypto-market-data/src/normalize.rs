use crate::error::MarketDataError;
use crate::messages::{BinancePartialDepthMessage, BinanceTickerMessage};
use crate::normalize::depth::normalize_binance_partial_depth;
use crate::normalize::payload::{unwrap_combined_payload, unwrap_combined_stream_payload};
use crate::normalize::ticker::normalize_binance_ticker;
use crate::stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig, BinanceStreamKind};
use crypto_domain::{MarketDepthSnapshot, MarketSnapshot, TimestampMs, TraceId};
use serde_json::Value;

mod depth;
mod math;
mod payload;
mod ticker;

pub(crate) use payload::{
    sequence_from_binance_payload, stream_kind_from_stream, stream_symbol, unwrap_binance_payload,
};

pub fn normalize_binance_ticker_message(
    config: &BinanceStreamConfig,
    raw_json: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<MarketSnapshot, MarketDataError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let payload = unwrap_combined_payload(value)?;
    let message: BinanceTickerMessage = serde_json::from_value(payload)?;
    normalize_binance_ticker(config, message, received_time_ms, decision_trace_id)
}

pub fn normalize_binance_partial_depth_message(
    config: &BinanceStreamConfig,
    raw_json: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<MarketDepthSnapshot, MarketDataError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let (stream, payload) = unwrap_combined_stream_payload(value)?;
    let message: BinancePartialDepthMessage = serde_json::from_value(payload)?;
    normalize_binance_partial_depth(
        config,
        &stream,
        message,
        received_time_ms,
        decision_trace_id,
    )
}

pub fn normalize_binance_stream_message(
    config: &BinanceStreamConfig,
    raw_json: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<BinanceNormalizedMarketEvent, MarketDataError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let payload = unwrap_binance_payload(value)?;
    let Some(stream) = payload.stream else {
        let message: BinanceTickerMessage = serde_json::from_value(payload.data)?;
        return normalize_binance_ticker(config, message, received_time_ms, decision_trace_id)
            .map(BinanceNormalizedMarketEvent::Market);
    };
    let stream_kind = stream_kind_from_stream(&stream)?;
    match stream_kind {
        BinanceStreamKind::Ticker => {
            let message: BinanceTickerMessage = serde_json::from_value(payload.data)?;
            normalize_binance_ticker(config, message, received_time_ms, decision_trace_id)
                .map(BinanceNormalizedMarketEvent::Market)
        }
        BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => {
            let message: BinancePartialDepthMessage = serde_json::from_value(payload.data)?;
            normalize_binance_partial_depth(
                config,
                &stream,
                message,
                received_time_ms,
                decision_trace_id,
            )
            .map(BinanceNormalizedMarketEvent::Depth)
        }
        BinanceStreamKind::Trade
        | BinanceStreamKind::BookTicker
        | BinanceStreamKind::DiffDepth100ms => Err(MarketDataError::InvalidMessage(format!(
            "{} stream is raw-ingest only and is not supported for replay normalization",
            stream_kind.name()
        ))),
    }
}
