use crate::binance::BinanceMarket;
use serde::Deserialize;

pub(super) const BINANCE_PAGE_LIMIT: usize = 1_000;

#[derive(Debug, Deserialize, Clone)]
pub(super) struct AggTrade {
    #[serde(rename = "a")]
    pub(super) aggregate_trade_id: i64,
    #[serde(rename = "p")]
    pub(super) price: String,
    #[serde(rename = "q")]
    pub(super) quantity: String,
    #[serde(rename = "f")]
    pub(super) first_trade_id: i64,
    #[serde(rename = "l")]
    pub(super) last_trade_id: i64,
    #[serde(rename = "T")]
    pub(super) trade_timestamp_ms: i64,
    #[serde(rename = "m")]
    pub(super) is_buyer_maker: bool,
    #[serde(rename = "M")]
    pub(super) is_best_match: bool,
}

pub(super) struct ResolvedBinanceBackfill {
    pub(super) rest_base_url: String,
    pub(super) markets: Vec<BinanceMarket>,
}
