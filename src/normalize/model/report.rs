use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct L1Manifest {
    pub schema_version: String,
    pub l1_run_id: String,
    pub status: String,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
    pub schema_version_emitted: String,
    pub report_key: String,
    pub output_object_keys: Vec<String>,
    #[serde(default)]
    pub market_data_quality_summary_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_summary_key: Option<String>,
    #[serde(default)]
    pub market_regime_context_key: Option<String>,
    #[serde(default)]
    pub symbol_universe_snapshot_key: Option<String>,
    #[serde(default)]
    pub symbol_universe_bootstrap_rollup_key: Option<String>,
    pub output_record_count: usize,
    pub slice_count_total: usize,
    pub finished_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NormalizationReport {
    pub schema_version: String,
    pub l1_run_id: String,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_schema_versions: Vec<String>,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_object_keys: Vec<String>,
    pub input_record_count: usize,
    pub duplicate_event_count: usize,
    pub invalid_event_count: usize,
    pub payload_hash_mismatch_count: usize,
    pub slice_count_total: usize,
    pub slice_count_complete: usize,
    pub slice_count_partial: usize,
    pub slice_count_incomplete: usize,
    pub slice_count_reference_only: usize,
    pub output_object_keys: Vec<String>,
    #[serde(default)]
    pub market_data_quality_summary_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_summary_key: Option<String>,
    #[serde(default)]
    pub market_regime_context_key: Option<String>,
    #[serde(default)]
    pub symbol_universe_snapshot_key: Option<String>,
    #[serde(default)]
    pub symbol_universe_bootstrap_rollup_key: Option<String>,
    pub status: String,
    pub failure_reason: Option<String>,
    pub manifest_key: String,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub runner_git_sha: String,
    pub runner_git_dirty: bool,
    pub runner_build_profile: String,
    pub schema_version_emitted: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketDataQualitySummary {
    pub schema_version: String,
    pub quality_summary_id: String,
    pub l1_run_id: String,
    pub coverage_ratio: f64,
    pub gap_count: i64,
    pub stale_sources: Vec<String>,
    pub delayed_sources: Vec<String>,
    pub missing_venues: Vec<String>,
    pub source_health_status: String,
    pub symbol_health_status: String,
    pub quality_window_start_ms: i64,
    pub quality_window_end_ms: i64,
    pub known_as_of_ms: i64,
}
