use super::parsed::{BinanceParsedEnvelope, BinanceParsedEvent};
use super::{
    BinanceBookTickerMessage, BinanceDiffDepthMessage, BinanceTickerMessage, BinanceTradeMessage,
};
use crate::binance::BinanceIngestError;
use crypto_market_data::BinanceStreamKind;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct BinanceCombinedMessage {
    stream: String,
    data: Value,
}

pub fn parse_binance_payload(raw_json: &str) -> Result<BinanceParsedEnvelope, BinanceIngestError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let payload_json = serde_json::to_string(&value)?;
    let combined: BinanceCombinedMessage = serde_json::from_value(value)?;
    let kind = stream_kind(&combined.stream)?;
    let event = match kind {
        BinanceStreamKind::Trade => serde_json::from_value::<BinanceTradeMessage>(combined.data)
            .map(BinanceParsedEvent::Trade)?,
        BinanceStreamKind::Ticker => serde_json::from_value::<BinanceTickerMessage>(combined.data)
            .map(BinanceParsedEvent::Ticker)?,
        BinanceStreamKind::BookTicker => {
            serde_json::from_value::<BinanceBookTickerMessage>(combined.data)
                .map(BinanceParsedEvent::BookTicker)?
        }
        BinanceStreamKind::DiffDepth100ms => {
            serde_json::from_value::<BinanceDiffDepthMessage>(combined.data)
                .map(BinanceParsedEvent::DiffDepth)?
        }
        BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => {
            return Err(BinanceIngestError::InvalidMessage(
                "Binance partial depth WS is disabled; use REST /api/v3/depth snapshots".to_owned(),
            ));
        }
    };
    Ok(BinanceParsedEnvelope {
        stream: combined.stream,
        event,
        payload_json,
    })
}

fn stream_kind(stream: &str) -> Result<BinanceStreamKind, BinanceIngestError> {
    let Some((_, suffix)) = stream.split_once('@') else {
        return Err(BinanceIngestError::InvalidMessage(format!(
            "invalid Binance stream name: {stream}"
        )));
    };
    match suffix {
        "trade" => Ok(BinanceStreamKind::Trade),
        "ticker" => Ok(BinanceStreamKind::Ticker),
        "bookTicker" => Ok(BinanceStreamKind::BookTicker),
        "depth@100ms" => Ok(BinanceStreamKind::DiffDepth100ms),
        "depth5" => Ok(BinanceStreamKind::PartialDepth5),
        "depth10" => Ok(BinanceStreamKind::PartialDepth10),
        "depth20" => Ok(BinanceStreamKind::PartialDepth20),
        _ => Err(BinanceIngestError::InvalidMessage(format!(
            "unsupported Binance stream suffix: {suffix}"
        ))),
    }
}
