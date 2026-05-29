use serde::{Deserialize, Serialize};

pub const LIGHTWEIGHT_AI_CANDIDATE_RANKER_SCHEMA_VERSION: &str =
    "lightweight_ai_candidate_ranker_v3";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateDecisionLabel {
    Promote,
    Retest,
    RiskOnly,
    Prune,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidatePerformanceVector {
    pub events: u64,
    pub avg_net_pnl_bps: Option<i64>,
    pub cumulative_net_pnl_bps: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateFeatureVector {
    pub search_surface: String,
    pub evaluation_surface: String,
    pub validation_split_method: String,
    pub validation_path_count: u64,
    pub grouped_validation_enabled: bool,
    pub proof_surface_required: bool,
    pub proof_surface_ready: bool,
    pub strategy_family: String,
    pub candidate_side: String,
    pub execution_model: String,
    pub symbol_count: u32,
    pub hedge_ratio_micros: i64,
    pub lookback_secs: i64,
    pub horizon_secs: i64,
    pub trigger_threshold_bps: Option<i64>,
    pub take_profit_bps: Option<i64>,
    pub stop_loss_bps: Option<i64>,
    pub entry_zscore_x100: i64,
    pub exit_zscore_x100: i64,
    pub volatility_regime_filter: String,
    pub hedge_ratio_stability_gate: String,
    pub residual_half_life_gate: String,
    pub liquidity_confirmation_gate: String,
    pub momentum_confirmation_gate: String,
    pub depth_confirmation_gate: String,
    pub context_selector: String,
    pub dominant_market_context: String,
    pub dominant_relative_strength_regime: String,
    pub dominant_volatility_state: String,
    pub dominant_trade_intensity_state: String,
    pub dominant_liquidity_tier: String,
    pub dominant_market_breadth_state: String,
    pub dominant_dispersion_state: String,
    pub dominant_depth_state: String,
    pub avg_relative_strength_bps: i64,
    pub avg_volatility_expansion_ppm: u32,
    pub avg_trade_intensity_expansion_ppm: u32,
    pub avg_imbalance_divergence_bps: i64,
    pub gross_edge_bps: i64,
    pub fee_drag_bps: i64,
    pub slippage_proxy_bps: i64,
    pub adverse_selection_proxy_bps: i64,
    pub depth_shortfall_proxy_bps: Option<i64>,
    pub train: CandidatePerformanceVector,
    pub validation: CandidatePerformanceVector,
    pub test: CandidatePerformanceVector,
    pub stress_train: CandidatePerformanceVector,
    pub stress_validation: CandidatePerformanceVector,
    pub stress_test: CandidatePerformanceVector,
    pub validation_test_min_avg_net_pnl_bps: Option<i64>,
    pub stress_validation_test_min_avg_net_pnl_bps: Option<i64>,
    pub positive_unseen_split_count: u32,
    pub stress_positive_unseen_split_count: u32,
    pub max_drawdown_bps: i64,
    pub event_count: u64,
    pub gate_reject_count: u64,
    pub gate_reject_rate_ppm: u64,
    pub promotion_failure_cause: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LightweightAiTrainingRow {
    pub schema_version: String,
    pub source_report: String,
    pub candidate_id: String,
    pub symbols: Vec<String>,
    pub feature_vector: CandidateFeatureVector,
    pub label: CandidateDecisionLabel,
    pub label_score: i64,
    pub allowed_use: String,
}
