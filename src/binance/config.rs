use crate::storage::L0StorageConfig;
use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind};

#[derive(Debug, Clone)]
pub struct BinanceMarket {
    pub raw_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}

#[derive(Debug, Clone)]
pub struct BinanceRunConfig {
    pub config_dir: String,
    pub rest_base_url: String,
    pub futures_rest_base_url: String,
    pub derivative_snapshot_interval_seconds: u64,
    pub stream_config: BinanceStreamConfig,
    pub markets: Vec<BinanceMarket>,
    pub duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub depth_snapshot_limit: u16,
    pub expect_symbol_count: usize,
    pub allow_partial_symbol_coverage: bool,
    pub stream_kinds: Vec<BinanceStreamKind>,
    pub l0_storage: Option<L0StorageConfig>,
}
