use super::BinanceIngestError;
use crypto_market_data::BinanceStreamKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceTradeMessage {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "T")]
    pub trade_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceTickerMessage {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceBookTickerMessage {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceDiffDepthMessage {
    #[serde(rename = "E")]
    pub event_time_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub final_update_id: i64,
}

#[derive(Debug, Clone, Serialize)]
pub enum BinanceParsedEvent {
    Trade(BinanceTradeMessage),
    Ticker(BinanceTickerMessage),
    BookTicker(BinanceBookTickerMessage),
    DiffDepth(BinanceDiffDepthMessage),
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceParsedEnvelope {
    pub stream: String,
    pub event: BinanceParsedEvent,
    pub payload_json: String,
}

#[derive(Debug, Deserialize)]
struct BinanceCombinedMessage {
    stream: String,
    data: Value,
}

impl BinanceParsedEvent {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Trade(_) => "trade",
            Self::Ticker(_) => "ticker",
            Self::BookTicker(_) => "bookTicker",
            Self::DiffDepth(_) => "depth@100ms",
        }
    }

    pub fn storage_event_type(&self) -> &'static str {
        match self {
            Self::Trade(_) => "trade",
            Self::Ticker(_) => "ticker",
            Self::BookTicker(_) => "book_ticker",
            Self::DiffDepth(_) => "depth_delta",
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Trade(message) => &message.symbol,
            Self::Ticker(message) => &message.symbol,
            Self::BookTicker(message) => &message.symbol,
            Self::DiffDepth(message) => &message.symbol,
        }
    }

    pub fn exchange_timestamp_ms(&self, fallback_ms: i64) -> i64 {
        match self {
            Self::Trade(message) => message.trade_time_ms,
            Self::Ticker(message) => message.event_time_ms,
            Self::BookTicker(_) => fallback_ms,
            Self::DiffDepth(message) => message.event_time_ms,
        }
    }

    pub fn sequence_id(&self) -> String {
        match self {
            Self::Trade(message) => format!("binance:trade:{}", message.trade_id),
            Self::Ticker(message) => format!("binance:ticker:{}", message.last_trade_id),
            Self::BookTicker(message) => format!("binance:book_ticker:{}", message.update_id),
            Self::DiffDepth(message) => format!("binance:depth_delta:{}", message.final_update_id),
        }
    }

    pub fn numeric_sequence(&self) -> i64 {
        match self {
            Self::Trade(message) => message.trade_id,
            Self::Ticker(message) => message.last_trade_id,
            Self::BookTicker(message) => message.update_id,
            Self::DiffDepth(message) => message.final_update_id,
        }
    }

    pub fn exchange_sequence(&self) -> Option<i64> {
        Some(self.numeric_sequence())
    }

    pub fn diff_first_update_id(&self) -> Option<i64> {
        match self {
            Self::DiffDepth(message) => Some(message.first_update_id),
            _ => None,
        }
    }

    pub fn diff_final_update_id(&self) -> Option<i64> {
        match self {
            Self::DiffDepth(message) => Some(message.final_update_id),
            _ => None,
        }
    }
}

pub fn parse_binance_payload(raw_json: &str) -> Result<BinanceParsedEnvelope, BinanceIngestError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let payload_json = serde_json::to_string(&value)?;
    let combined: BinanceCombinedMessage = serde_json::from_value(value)?;
    let kind = stream_kind(&combined.stream)?;
    let event = match kind {
        BinanceStreamKind::Trade => {
            serde_json::from_value(combined.data).map(BinanceParsedEvent::Trade)?
        }
        BinanceStreamKind::Ticker => {
            serde_json::from_value(combined.data).map(BinanceParsedEvent::Ticker)?
        }
        BinanceStreamKind::BookTicker => {
            serde_json::from_value(combined.data).map(BinanceParsedEvent::BookTicker)?
        }
        BinanceStreamKind::DiffDepth100ms => {
            serde_json::from_value(combined.data).map(BinanceParsedEvent::DiffDepth)?
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_trade() {
        let envelope = parse_binance_payload(
            r#"{"stream":"btcusdt@trade","data":{"E":1,"s":"BTCUSDT","t":42,"T":2}}"#,
        )
        .unwrap();

        assert_eq!(envelope.event.storage_event_type(), "trade");
        assert_eq!(envelope.event.symbol(), "BTCUSDT");
        assert_eq!(envelope.event.sequence_id(), "binance:trade:42");
    }

    #[test]
    fn parses_combined_depth_delta() {
        let envelope = parse_binance_payload(
            r#"{"stream":"btcusdt@depth@100ms","data":{"E":1,"s":"BTCUSDT","U":41,"u":42}}"#,
        )
        .unwrap();

        assert_eq!(envelope.event.storage_event_type(), "depth_delta");
        assert_eq!(envelope.event.numeric_sequence(), 42);
        assert_eq!(envelope.event.diff_first_update_id(), Some(41));
        assert_eq!(envelope.event.diff_final_update_id(), Some(42));
    }
}
