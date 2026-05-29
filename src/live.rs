use serde::{Deserialize, Serialize};

mod publisher;
mod quote;
#[cfg(test)]
mod tests;
mod tick;

pub use publisher::LiveMarketPublisher;
pub use tick::MarketLiveTick;

pub const MARKET_LIVE_TICK_SCHEMA_VERSION: &str = "market_live_tick_v1";
pub const DEFAULT_MARKET_LIVE_NATS_STREAM: &str = "MARKET_LIVE";
pub const DEFAULT_MARKET_LIVE_NATS_SUBJECT_PREFIX: &str = "market_live_tick.created";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveMarketNatsConfig {
    pub url: String,
    pub stream: String,
    pub subject_prefix: String,
    pub required: bool,
}
