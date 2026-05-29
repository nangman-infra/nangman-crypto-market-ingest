use super::MARKET_LIVE_TICK_SCHEMA_VERSION;
use super::quote::{midpoint, quote_from_payload};
use crate::storage::record::RawMarketEventRecord;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketLiveTick {
    pub schema_version: String,
    pub event_id: String,
    pub producer_run_id: String,
    pub venue: String,
    pub source_role: String,
    pub market_type: String,
    pub event_type: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub latency_ms: i64,
    pub sequence_id: String,
    pub sequence_tag: String,
    pub price_source: String,
    pub last_price: Option<f64>,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub trade_volume: Option<f64>,
    pub payload_sha256: String,
}

impl MarketLiveTick {
    pub fn from_raw_market_event(record: &RawMarketEventRecord) -> Self {
        let payload = serde_json::from_str::<Value>(&record.payload_json).ok();
        let quote = payload
            .as_ref()
            .and_then(|value| quote_from_payload(record, value))
            .unwrap_or_default();
        let mark_price = quote
            .last_price
            .or_else(|| midpoint(quote.best_bid_price, quote.best_ask_price));
        Self {
            schema_version: MARKET_LIVE_TICK_SCHEMA_VERSION.to_owned(),
            event_id: record.event_id.clone(),
            producer_run_id: record.producer_run_id.clone(),
            venue: record.venue.clone(),
            source_role: record.source_role.clone(),
            market_type: record.market_type.clone(),
            event_type: record.event_type.clone(),
            symbol_native: record.symbol_native.clone(),
            symbol_canonical: record.symbol_canonical.clone(),
            base_asset: record.base_asset.clone(),
            quote_asset: record.quote_asset.clone(),
            exchange_timestamp_ms: record.exchange_timestamp_ms,
            ingest_timestamp_ms: record.ingest_timestamp_ms,
            latency_ms: record
                .ingest_timestamp_ms
                .saturating_sub(record.exchange_timestamp_ms),
            sequence_id: record.sequence_id.clone(),
            sequence_tag: record.sequence_tag.clone(),
            price_source: quote.price_source,
            last_price: quote.last_price,
            best_bid_price: quote.best_bid_price,
            best_ask_price: quote.best_ask_price,
            mark_price,
            trade_volume: quote.trade_volume,
            payload_sha256: record.payload_sha256.clone(),
        }
    }

    pub fn has_mark_price(&self) -> bool {
        self.mark_price
            .is_some_and(|value| value.is_finite() && value > 0.0)
    }
}
