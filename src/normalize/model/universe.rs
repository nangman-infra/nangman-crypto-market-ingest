use serde::{Deserialize, Serialize};

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
