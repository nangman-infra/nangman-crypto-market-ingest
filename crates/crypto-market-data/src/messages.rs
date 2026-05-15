use crypto_domain::{Sequence, TimestampMs};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BinanceCombinedMessage {
    pub(crate) stream: String,
    pub(crate) data: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BinanceTickerMessage {
    #[serde(rename = "e")]
    pub(crate) event_type: String,
    #[serde(rename = "E")]
    pub(crate) event_time_ms: TimestampMs,
    #[serde(rename = "s")]
    pub(crate) symbol: String,
    #[serde(rename = "c")]
    pub(crate) last_price: String,
    #[serde(rename = "b")]
    pub(crate) best_bid: String,
    #[serde(rename = "B")]
    pub(crate) best_bid_qty: String,
    #[serde(rename = "a")]
    pub(crate) best_ask: String,
    #[serde(rename = "A")]
    pub(crate) best_ask_qty: String,
    #[serde(rename = "L")]
    pub(crate) last_trade_id: Sequence,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BinancePartialDepthMessage {
    #[serde(rename = "lastUpdateId")]
    pub(crate) last_update_id: Sequence,
    pub(crate) bids: Vec<[String; 2]>,
    pub(crate) asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BinanceDiffDepthMessage {
    #[serde(rename = "s")]
    pub(crate) symbol: String,
    #[serde(rename = "U")]
    pub(crate) first_update_id: Sequence,
    #[serde(rename = "u")]
    pub(crate) final_update_id: Sequence,
    #[serde(rename = "b")]
    pub(crate) bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub(crate) asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BinanceOrderBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub(crate) last_update_id: Sequence,
    pub(crate) bids: Vec<[String; 2]>,
    pub(crate) asks: Vec<[String; 2]>,
}

pub(crate) struct BinancePayload {
    pub(crate) stream: Option<String>,
    pub(crate) data: Value,
}
