use std::path::PathBuf;

pub use self::parse_cli::parse_args;

mod env;
mod help;
mod parse;
mod parse_cli;
#[cfg(test)]
mod tests;
mod validation;

const DEFAULT_CONFIG_DIR: &str = "/opt/nangman-crypto/strategies/crypto/rust-engine/config";
const DEFAULT_L0_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
const DEFAULT_BINANCE_FUTURES_REST_BASE_URL: &str = "https://fapi.binance.com";
const DEFAULT_BINANCE_DERIVATIVES_SNAPSHOT_INTERVAL_SECONDS: u64 = 300;
const DEFAULT_HIGH_WATER_PCT: u8 = 70;
const DEFAULT_EMERGENCY_PCT: u8 = 90;
const DEFAULT_SAFETY_FLOOR_HOURS: i64 = 2;
const DEFAULT_EVICTION_CHECK_INTERVAL_SECS: u64 = 600;
const DEFAULT_S3_RETENTION_DAYS: i64 = 45;
const DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS: u64 = 21_600;
const DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN: usize = 1_000;

#[derive(Debug)]
pub struct Args {
    pub venue: Venue,
    pub config_dir: PathBuf,
    pub duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub depth_snapshot_limit: u16,
    pub expect_symbol_count: usize,
    pub allow_partial_symbol_coverage: bool,
    pub binance_futures_rest_base_url: String,
    pub binance_derivatives_snapshot_interval_seconds: u64,
    pub upbit_rest_base_url: Option<String>,
    pub upbit_websocket_url: Option<String>,
    pub upbit_quote_currency: String,
    pub upbit_orderbook_unit: u8,
    pub l0_s3_bucket: Option<String>,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
    pub local_disk_high_water_pct: u8,
    pub local_disk_emergency_pct: u8,
    pub safety_floor_hours: i64,
    pub eviction_check_interval_secs: u64,
    pub s3_retention_enabled: bool,
    pub s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
    pub live_nats_url: Option<String>,
    pub live_nats_stream: String,
    pub live_nats_subject_prefix: String,
    pub live_nats_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Binance,
    Upbit,
}

pub fn print_help() {
    help::print_help();
}
