use crate::error::MarketDataError;
use crate::messages::{BinanceCombinedMessage, BinancePayload};
use crate::stream_config::BinanceStreamKind;
use crypto_domain::Sequence;
use serde_json::Value;

pub(crate) fn sequence_from_binance_payload(
    kind: BinanceStreamKind,
    payload: &Value,
) -> Option<Sequence> {
    let field = match kind {
        BinanceStreamKind::Trade => "t",
        BinanceStreamKind::Ticker => "L",
        BinanceStreamKind::BookTicker => "u",
        BinanceStreamKind::DiffDepth100ms => "u",
        BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => "lastUpdateId",
    };
    payload.get(field)?.as_u64()
}

pub(crate) fn stream_symbol(stream: &str) -> Option<&str> {
    let symbol = stream.split('@').next()?.trim();
    (!symbol.is_empty()).then_some(symbol)
}

pub(crate) fn unwrap_binance_payload(value: Value) -> Result<BinancePayload, MarketDataError> {
    if value.get("stream").is_some() && value.get("data").is_some() {
        let combined: BinanceCombinedMessage = serde_json::from_value(value)?;
        if combined.stream.trim().is_empty() {
            return Err(MarketDataError::InvalidMessage(
                "combined stream name is empty".to_owned(),
            ));
        }
        return Ok(BinancePayload {
            stream: Some(combined.stream),
            data: combined.data,
        });
    }
    Ok(BinancePayload {
        stream: None,
        data: value,
    })
}

pub(crate) fn stream_kind_from_stream(stream: &str) -> Result<BinanceStreamKind, MarketDataError> {
    let suffix = stream
        .split_once('@')
        .map(|(_, suffix)| suffix)
        .ok_or_else(|| MarketDataError::InvalidMessage(format!("unsupported stream: {stream}")))?;
    match suffix {
        "trade" => Ok(BinanceStreamKind::Trade),
        "ticker" => Ok(BinanceStreamKind::Ticker),
        "depth@100ms" => Ok(BinanceStreamKind::DiffDepth100ms),
        "depth5" => Ok(BinanceStreamKind::PartialDepth5),
        "depth10" => Ok(BinanceStreamKind::PartialDepth10),
        "depth20" => Ok(BinanceStreamKind::PartialDepth20),
        "bookTicker" => Ok(BinanceStreamKind::BookTicker),
        _ => Err(MarketDataError::InvalidMessage(format!(
            "unsupported stream: {stream}"
        ))),
    }
}

pub(super) fn unwrap_combined_payload(value: Value) -> Result<Value, MarketDataError> {
    Ok(unwrap_binance_payload(value)?.data)
}

pub(super) fn unwrap_combined_stream_payload(
    value: Value,
) -> Result<(String, Value), MarketDataError> {
    let payload = unwrap_binance_payload(value)?;
    let stream = payload.stream.ok_or_else(|| {
        MarketDataError::InvalidMessage(
            "combined stream wrapper is required for partial depth payload".to_owned(),
        )
    })?;
    Ok((stream, payload.data))
}
