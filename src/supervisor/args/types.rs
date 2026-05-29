use super::DEFAULT_L0_RUN_KEY_OVERLAP_MS;
use super::defaults::{
    DEFAULT_AWS_REGION, DEFAULT_BACKFILL_BIN, DEFAULT_BOOTSTRAP_CHUNK_HOURS,
    DEFAULT_BOOTSTRAP_INTERVAL_SECS, DEFAULT_BOOTSTRAP_LOOKBACK_DAYS, DEFAULT_CATCHUP_TMP_ROOT,
    DEFAULT_CONFIG_DIR, DEFAULT_L0_S3_RETENTION_DAYS, DEFAULT_L0_SPOOL_ROOT,
    DEFAULT_L1_S3_RETENTION_DAYS, DEFAULT_L1_SPOOL_ROOT, DEFAULT_NORMALIZE_BIN,
    DEFAULT_REALTIME_BIN, DEFAULT_REALTIME_DURATION_SECONDS, DEFAULT_RESTART_DELAY_SECS,
    DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS, DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
};
use super::env_config::{env_bool, env_string};
use crate::live::{DEFAULT_MARKET_LIVE_NATS_STREAM, DEFAULT_MARKET_LIVE_NATS_SUBJECT_PREFIX};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SupervisorArgs {
    pub config_dir: PathBuf,
    pub l0_s3_bucket: String,
    pub l1_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l1_spool_root: PathBuf,
    pub catchup_tmp_root: PathBuf,
    pub realtime_bin: PathBuf,
    pub backfill_bin: PathBuf,
    pub normalize_bin: PathBuf,
    pub realtime_venue: String,
    pub realtime_venues: Vec<String>,
    pub expect_symbol_count: usize,
    pub realtime_duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
    pub bootstrap_enabled: bool,
    pub bootstrap_lookback_days: i64,
    pub bootstrap_chunk_hours: i64,
    pub bootstrap_interval_secs: u64,
    pub bootstrap_symbols: Option<Vec<String>>,
    pub normalize_schedule_interval_ms: i64,
    pub l0_run_key_overlap_ms: i64,
    pub normalize_max_windows_per_tick: usize,
    pub l0_s3_retention_days: i64,
    pub l1_s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
    pub restart_delay_secs: u64,
    pub live_nats_url: Option<String>,
    pub live_nats_stream: String,
    pub live_nats_subject_prefix: String,
    pub live_nats_required: bool,
}

impl SupervisorArgs {
    pub(super) fn with_defaults() -> Self {
        Self {
            config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
            l0_s3_bucket: String::new(),
            l1_s3_bucket: String::new(),
            aws_profile: None,
            aws_region: DEFAULT_AWS_REGION.to_owned(),
            l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
            l1_spool_root: PathBuf::from(DEFAULT_L1_SPOOL_ROOT),
            catchup_tmp_root: PathBuf::from(DEFAULT_CATCHUP_TMP_ROOT),
            realtime_bin: PathBuf::from(DEFAULT_REALTIME_BIN),
            backfill_bin: PathBuf::from(DEFAULT_BACKFILL_BIN),
            normalize_bin: PathBuf::from(DEFAULT_NORMALIZE_BIN),
            realtime_venue: "binance".to_owned(),
            realtime_venues: vec!["binance".to_owned()],
            expect_symbol_count: 50,
            realtime_duration_seconds: DEFAULT_REALTIME_DURATION_SECONDS,
            log_interval_seconds: 30,
            l0_flush_records: 1_000,
            l0_shard_count: 1,
            bootstrap_enabled: true,
            bootstrap_lookback_days: DEFAULT_BOOTSTRAP_LOOKBACK_DAYS,
            bootstrap_chunk_hours: DEFAULT_BOOTSTRAP_CHUNK_HOURS,
            bootstrap_interval_secs: DEFAULT_BOOTSTRAP_INTERVAL_SECS,
            bootstrap_symbols: None,
            normalize_schedule_interval_ms: 900_000,
            l0_run_key_overlap_ms: DEFAULT_L0_RUN_KEY_OVERLAP_MS,
            normalize_max_windows_per_tick: 192,
            l0_s3_retention_days: DEFAULT_L0_S3_RETENTION_DAYS,
            l1_s3_retention_days: DEFAULT_L1_S3_RETENTION_DAYS,
            s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
            s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
            restart_delay_secs: DEFAULT_RESTART_DELAY_SECS,
            live_nats_url: env_string("MARKET_LIVE_NATS_URL"),
            live_nats_stream: env_string("MARKET_LIVE_NATS_STREAM")
                .unwrap_or_else(|| DEFAULT_MARKET_LIVE_NATS_STREAM.to_owned()),
            live_nats_subject_prefix: env_string("MARKET_LIVE_NATS_SUBJECT_PREFIX")
                .unwrap_or_else(|| DEFAULT_MARKET_LIVE_NATS_SUBJECT_PREFIX.to_owned()),
            live_nats_required: env_bool("MARKET_LIVE_NATS_REQUIRED"),
        }
    }
}
