use serde::{Deserialize, Serialize};

pub const SLICE_SCHEMA_VERSION: &str = "normalized_market_slice_v1";
pub const REPORT_SCHEMA_VERSION: &str = "normalization_report_v1";
pub const MANIFEST_SCHEMA_VERSION: &str = "l1_manifest_v1";
pub const MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION: &str = "market_data_quality_summary_v1";
pub const MARKET_FEATURE_DELTA_SCHEMA_VERSION: &str = "market_feature_delta_v1";
pub const MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION: &str = "market_feature_delta_summary_v1";
pub const MARKET_REGIME_CONTEXT_SCHEMA_VERSION: &str = "market_regime_context_v1";
pub const SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION: &str =
    "symbol_universe_bootstrap_rollup_v1";
pub const SYMBOL_UNIVERSE_SNAPSHOT_SCHEMA_VERSION: &str = "symbol_universe_snapshot_v1";

#[derive(Debug, Clone)]
pub struct RawInputEvent {
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
    pub exchange_sequence: Option<i64>,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct SymbolHealthInput {
    pub venue: String,
    pub symbol_native: String,
    pub observed_at_ms: i64,
    pub last_event_time_ms: i64,
    pub latency_ms: i64,
    pub is_tradeable: bool,
    pub reason_codes: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct SourceHealthInput {
    pub venue: String,
    pub observed_at_ms: i64,
    pub connection_status: String,
    pub heartbeat_delay_ms: i64,
    pub stream_lag_ms: i64,
    pub recent_gap_count: i64,
    pub book_rebuild_count: i64,
    pub health_level: String,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct GapAlertInput {
    pub venue: String,
    pub symbol_native: String,
    pub gap_type: String,
    pub detected_at_ms: i64,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeNormalized {
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub price: f64,
    pub quantity: f64,
    pub side: String,
    pub exchange_sequence: Option<i64>,
    pub parent_event_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BookTickerNormalized {
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
    pub exchange_sequence: Option<i64>,
    pub parent_event_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactEventRef {
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub event_type: String,
    pub parent_event_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DerivativeMetricObservation {
    pub venue: String,
    pub source_role: String,
    pub market_type: String,
    pub metric_name: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub value: f64,
    pub unit: String,
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub parent_event_id: String,
    pub parent_run_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolHealthSnapshot {
    pub observed_at_ms: i64,
    pub last_event_time_ms: i64,
    pub last_received_time_ms: i64,
    pub latency_ms: i64,
    pub is_tradeable: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceHealthSnapshot {
    pub observed_at_ms: i64,
    pub connection_status: String,
    pub health_level: String,
    pub heartbeat_delay_ms: i64,
    pub stream_lag_ms: i64,
    pub recent_gap_count: i64,
    pub book_rebuild_count: i64,
}

#[derive(Debug, Clone)]
pub struct SliceRow {
    pub slice_id: String,
    pub venue: String,
    pub source_role: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub market_type: String,
    pub window_ms: i64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub slice_completeness: String,
    pub missing_reasons: Vec<String>,
    pub quality_ok: i64,
    pub quality_delayed: i64,
    pub quality_stale: i64,
    pub quality_gap: i64,
    pub quality_invalid: i64,
    pub trade_count: i64,
    pub trade_volume: f64,
    pub last_trade_price: Option<f64>,
    pub last_trade_size: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid_price: Option<f64>,
    pub spread_bps: Option<f64>,
    pub book_ticker_count: i64,
    pub depth_event_count: i64,
    pub depth_book_rebuilt: bool,
    pub trade_events: Vec<TradeNormalized>,
    pub book_ticker_events: Vec<BookTickerNormalized>,
    pub depth_events: Vec<CompactEventRef>,
    pub ticker_events: Vec<CompactEventRef>,
    pub symbol_health_snapshot: Option<SymbolHealthSnapshot>,
    pub source_health_snapshot: Option<SourceHealthSnapshot>,
    pub parent_event_ids: Vec<String>,
    pub parent_run_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NormalizeInputs {
    pub raw_events: Vec<RawInputEvent>,
    pub symbol_health: Vec<SymbolHealthInput>,
    pub source_health: Vec<SourceHealthInput>,
    pub gap_alerts: Vec<GapAlertInput>,
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_object_keys: Vec<String>,
}

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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketFeatureDelta {
    pub schema_version: String,
    pub feature_delta_id: String,
    pub l1_run_id: String,
    pub metric_name: String,
    pub venue: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub market_type: String,
    pub value_now: f64,
    pub value_15m_ago: Option<f64>,
    pub value_1h_ago: Option<f64>,
    pub change_pct_15m: Option<f64>,
    pub change_pct_1h: Option<f64>,
    pub price_change_same_window: Option<f64>,
    pub volume_change_same_window: Option<f64>,
    pub oi_price_divergence: Option<f64>,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub known_as_of_ms: i64,
    pub quality_status: String,
    pub missing_reasons: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketFeatureDeltaSummary {
    pub schema_version: String,
    pub feature_delta_summary_id: String,
    pub l1_run_id: String,
    pub detail_feature_delta_key: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub known_as_of_ms: i64,
    pub detail_record_count: usize,
    pub summary_row_count: usize,
    pub rows: Vec<MarketFeatureDeltaSummaryRow>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketFeatureDeltaSummaryRow {
    pub venue: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub market_type: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub known_as_of_ms: i64,
    pub quality_status: String,
    pub missing_reasons: Vec<String>,
    pub metrics: Vec<MarketFeatureDeltaSummaryMetric>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketFeatureDeltaSummaryMetric {
    pub metric_name: String,
    pub value_now: f64,
    pub value_15m_ago: Option<f64>,
    pub value_1h_ago: Option<f64>,
    pub change_pct_15m: Option<f64>,
    pub change_pct_1h: Option<f64>,
    pub price_change_same_window: Option<f64>,
    pub volume_change_same_window: Option<f64>,
    pub oi_price_divergence: Option<f64>,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub quality_status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketRegimeContext {
    pub schema_version: String,
    pub regime_context_id: String,
    pub l1_run_id: String,
    pub scope: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub btc_return_same_window: Option<f64>,
    pub eth_return_same_window: Option<f64>,
    pub sector_return_same_window: Option<f64>,
    pub volatility_regime: String,
    pub correlation_to_btc: Option<f64>,
    pub known_as_of_ms: i64,
    pub quality_status: String,
    pub missing_reasons: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SymbolUniverseSnapshot {
    pub schema_version: String,
    pub symbol_universe_snapshot_id: String,
    pub universe_as_of_ms: i64,
    pub included_symbols: Vec<SymbolUniverseMember>,
    pub excluded_symbols: Vec<SymbolUniverseMember>,
    pub liquidity_rank_at_that_time: Vec<SymbolLiquidityRank>,
    pub selection_policy_version: String,
    pub venue_truth_policy_version: String,
    pub data_quality_cutoff_version: String,
    pub generated_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SymbolUniverseBootstrapRollup {
    pub schema_version: String,
    pub rollup_id: String,
    pub event_date: String,
    pub day_start_ms: i64,
    pub generated_at_ms: i64,
    pub updated_by_l1_run_id: String,
    pub source_windows: Vec<SymbolUniverseBootstrapSourceWindow>,
    pub symbols: Vec<SymbolUniverseBootstrapSymbolStats>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SymbolUniverseBootstrapSourceWindow {
    pub l1_run_id: String,
    pub source_window_start_ms: i64,
    pub source_window_end_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SymbolUniverseBootstrapSymbolStats {
    pub symbol_canonical: String,
    pub execution_symbol_native: Option<String>,
    pub reference_symbol_native: Option<String>,
    pub traded_notional_sum: f64,
    pub spread_bps_median_samples: Vec<f64>,
    pub gap_count: i64,
    pub window_count: i64,
    pub mapping_confidence: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SymbolUniverseMember {
    pub symbol_canonical: String,
    pub execution_symbol_native: Option<String>,
    pub reference_symbol_native: Option<String>,
    pub liquidity_rank_at_that_time: Option<i64>,
    pub approved_universe_symbol: bool,
    pub bootstrap_days_available: i64,
    pub median_spread_bps_30d: Option<f64>,
    pub median_traded_notional_30d: Option<f64>,
    pub gap_rate_30d: Option<f64>,
    pub mapping_confidence: String,
    pub status_reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SymbolLiquidityRank {
    pub symbol_canonical: String,
    pub liquidity_rank_at_that_time: i64,
    pub observed_traded_notional: f64,
}
