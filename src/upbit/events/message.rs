use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitTickerMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub code: String,
    pub timestamp: i64,
    pub trade_timestamp: Option<i64>,
    pub trade_price: Option<f64>,
    pub acc_trade_price_24h: Option<f64>,
    pub acc_trade_volume_24h: Option<f64>,
    pub stream_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitTradeMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub code: String,
    pub timestamp: i64,
    pub trade_timestamp: i64,
    pub trade_price: f64,
    pub trade_volume: f64,
    pub ask_bid: String,
    pub sequential_id: i64,
    pub best_ask_price: Option<f64>,
    pub best_ask_size: Option<f64>,
    pub best_bid_price: Option<f64>,
    pub best_bid_size: Option<f64>,
    pub stream_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderbookMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub code: String,
    pub timestamp: i64,
    pub total_ask_size: f64,
    pub total_bid_size: f64,
    #[serde(default)]
    pub orderbook_units: Vec<UpbitOrderbookUnit>,
    pub stream_type: Option<String>,
    pub level: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderbookUnit {
    pub ask_price: f64,
    pub bid_price: f64,
    pub ask_size: f64,
    pub bid_size: f64,
}
