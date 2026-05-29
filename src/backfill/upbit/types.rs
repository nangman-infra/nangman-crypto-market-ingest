use serde::Deserialize;

#[derive(Debug, Clone)]
pub(super) struct UpbitBackfillMarket {
    pub(super) market: String,
    pub(super) base_asset: String,
    pub(super) quote_asset: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(super) struct UpbitTrade {
    pub(super) market: String,
    pub(super) timestamp: i64,
    pub(super) trade_price: f64,
    pub(super) trade_volume: f64,
    pub(super) ask_bid: String,
    pub(super) sequential_id: i64,
}

#[derive(Debug, Clone)]
pub(super) struct UpbitInitialCursor {
    pub(super) to: String,
    pub(super) days_ago: Option<i64>,
}
