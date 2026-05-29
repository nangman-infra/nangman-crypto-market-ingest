use super::message::{
    BinanceBookTickerMessage, BinanceDiffDepthMessage, BinanceTickerMessage, BinanceTradeMessage,
};
use serde::Serialize;

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
