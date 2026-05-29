use super::message::{UpbitOrderbookMessage, UpbitTickerMessage, UpbitTradeMessage};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub enum UpbitParsedEvent {
    Ticker(UpbitTickerMessage),
    Trade(UpbitTradeMessage),
    Orderbook(UpbitOrderbookMessage),
    Status(String),
    Error { name: String, message: String },
    Unknown(Value),
}

#[derive(Debug, Clone, Serialize)]
pub struct UpbitParsedEnvelope {
    pub event: UpbitParsedEvent,
    pub payload_json: String,
}

impl UpbitParsedEvent {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Ticker(_) => "ticker",
            Self::Trade(_) => "trade",
            Self::Orderbook(_) => "orderbook",
            Self::Status(_) => "status",
            Self::Error { .. } => "error",
            Self::Unknown(_) => "unknown",
        }
    }

    pub fn symbol(&self) -> Option<&str> {
        match self {
            Self::Ticker(message) => Some(&message.code),
            Self::Trade(message) => Some(&message.code),
            Self::Orderbook(message) => Some(&message.code),
            Self::Status(_) | Self::Error { .. } | Self::Unknown(_) => None,
        }
    }

    pub fn exchange_timestamp_ms(&self) -> Option<i64> {
        match self {
            Self::Ticker(message) => Some(message.timestamp),
            Self::Trade(message) => Some(message.trade_timestamp),
            Self::Orderbook(message) => Some(message.timestamp),
            Self::Status(_) | Self::Error { .. } | Self::Unknown(_) => None,
        }
    }
}
