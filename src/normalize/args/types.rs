use std::path::PathBuf;

use super::defaults::{
    DEFAULT_AWS_REGION, DEFAULT_CATCHUP_TMP_ROOT, DEFAULT_CLOCK_SKEW_MARGIN_MS,
    DEFAULT_L0_LOCAL_ROOT, DEFAULT_L0_RUN_KEY_OVERLAP_MS, DEFAULT_L0_S3_RETENTION_DAYS,
    DEFAULT_L1_INDEX_UPLOAD_CONCURRENCY, DEFAULT_L1_S3_RETENTION_DAYS, DEFAULT_L1_SPOOL_ROOT,
    DEFAULT_LIVE_PRIORITY_LAG_THRESHOLD_MS, DEFAULT_MAX_LATENCY_MS, DEFAULT_MAX_WINDOWS_PER_TICK,
    DEFAULT_PROJECTION_LOOKBACK_MS, DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
    DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN, DEFAULT_SCAN_MARGIN_MS, DEFAULT_SCHEDULE_INTERVAL_MS,
    DEFAULT_WATERMARK_DELAY_MS, DEFAULT_WINDOW_MS,
};

#[derive(Debug, Clone)]
pub struct NormalizeArgs {
    pub l0_s3_bucket: String,
    pub l0_local_root: PathBuf,
    pub l1_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub input_start_ms: Option<i64>,
    pub input_end_ms: Option<i64>,
    pub schedule_interval_ms: i64,
    pub window_ms: i64,
    pub scan_margin_ms: i64,
    pub projection_lookback_ms: i64,
    pub watermark_delay_ms: i64,
    pub clock_skew_margin_ms: i64,
    pub max_latency_ms: i64,
    pub l0_run_key_overlap_ms: i64,
    pub spool_root: PathBuf,
    pub catchup_tmp_root: PathBuf,
    pub preflight: bool,
    pub audit_l1_index_start_ms: Option<i64>,
    pub audit_l1_index_end_ms: Option<i64>,
    pub max_windows_per_tick: usize,
    pub live_priority: bool,
    pub live_priority_only: bool,
    pub live_priority_lag_threshold_ms: i64,
    pub s3_retention_enabled: bool,
    pub l0_s3_retention_days: i64,
    pub l1_s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
    pub l1_index_upload_concurrency: usize,
}

impl NormalizeArgs {
    pub(super) fn with_defaults() -> Self {
        Self {
            l0_s3_bucket: String::new(),
            l0_local_root: PathBuf::from(DEFAULT_L0_LOCAL_ROOT),
            l1_s3_bucket: String::new(),
            aws_profile: None,
            aws_region: DEFAULT_AWS_REGION.to_owned(),
            input_start_ms: None,
            input_end_ms: None,
            schedule_interval_ms: DEFAULT_SCHEDULE_INTERVAL_MS,
            window_ms: DEFAULT_WINDOW_MS,
            scan_margin_ms: DEFAULT_SCAN_MARGIN_MS,
            projection_lookback_ms: DEFAULT_PROJECTION_LOOKBACK_MS,
            watermark_delay_ms: DEFAULT_WATERMARK_DELAY_MS,
            clock_skew_margin_ms: DEFAULT_CLOCK_SKEW_MARGIN_MS,
            max_latency_ms: DEFAULT_MAX_LATENCY_MS,
            l0_run_key_overlap_ms: DEFAULT_L0_RUN_KEY_OVERLAP_MS,
            spool_root: PathBuf::from(DEFAULT_L1_SPOOL_ROOT),
            catchup_tmp_root: PathBuf::from(DEFAULT_CATCHUP_TMP_ROOT),
            preflight: false,
            audit_l1_index_start_ms: None,
            audit_l1_index_end_ms: None,
            max_windows_per_tick: DEFAULT_MAX_WINDOWS_PER_TICK,
            live_priority: false,
            live_priority_only: false,
            live_priority_lag_threshold_ms: DEFAULT_LIVE_PRIORITY_LAG_THRESHOLD_MS,
            s3_retention_enabled: true,
            l0_s3_retention_days: DEFAULT_L0_S3_RETENTION_DAYS,
            l1_s3_retention_days: DEFAULT_L1_S3_RETENTION_DAYS,
            s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
            s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
            l1_index_upload_concurrency: DEFAULT_L1_INDEX_UPLOAD_CONCURRENCY,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputRange {
    pub start_ms: i64,
    pub end_ms: i64,
}
