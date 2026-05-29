use serde::{Deserialize, Serialize};

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
