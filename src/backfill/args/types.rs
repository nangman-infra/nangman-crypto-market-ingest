use super::defaults::{
    DEFAULT_AWS_REGION, DEFAULT_CONFIG_DIR, DEFAULT_L0_SPOOL_ROOT, DEFAULT_S3_RETENTION_DAYS,
    DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN, DEFAULT_UPBIT_QUOTE_CURRENCY,
};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct BackfillArgs {
    pub venue: Venue,
    pub config_dir: PathBuf,
    pub rest_base_url: Option<String>,
    pub input_start_ms: i64,
    pub input_end_ms: i64,
    pub expect_symbol_count: usize,
    pub symbols: Option<Vec<String>>,
    pub upbit_quote_currency: String,
    pub l0_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
    pub s3_retention_enabled: bool,
    pub s3_retention_days: i64,
    pub s3_retention_max_deletes_per_run: usize,
}

impl BackfillArgs {
    pub(super) fn with_defaults() -> Self {
        Self {
            venue: Venue::Binance,
            config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
            rest_base_url: None,
            input_start_ms: 0,
            input_end_ms: 0,
            expect_symbol_count: 50,
            symbols: None,
            upbit_quote_currency: DEFAULT_UPBIT_QUOTE_CURRENCY.to_owned(),
            l0_s3_bucket: String::new(),
            aws_profile: None,
            aws_region: DEFAULT_AWS_REGION.to_owned(),
            l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
            l0_flush_records: 1_000,
            l0_shard_count: 1,
            s3_retention_enabled: true,
            s3_retention_days: DEFAULT_S3_RETENTION_DAYS,
            s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Binance,
    Upbit,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Upbit => "upbit",
        }
    }
}
