use crate::{
    Bps, DepthAttachmentStatus, DepthMissingReason, MicroBps, ReasonCode, Symbol, TraceId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RollingFeatures {
    pub decision_trace_id: TraceId,
    pub symbol: Symbol,
    pub window_ms: i64,
    pub spread_bps: Bps,
    pub liquidity_score_ppm: u32,
    pub orderbook_imbalance_bps: Bps,
    pub volatility_bps: Bps,
    pub jump_score_bps: Bps,
    pub short_horizon_price_impact_bps: Bps,
    pub short_horizon_price_impact_3_tick_bps: Bps,
    pub short_horizon_price_impact_10_tick_bps: Bps,
    pub microprice_edge_bps: Bps,
    pub microprice_edge_micro_bps: MicroBps,
    #[serde(default)]
    pub volatility_expansion_bps: Bps,
    #[serde(default)]
    pub trade_intensity_60s: u32,
    #[serde(default)]
    pub trade_intensity_previous_60s: u32,
    #[serde(default)]
    pub trade_intensity_expansion_ppm: u32,
    #[serde(default)]
    pub symbol_relative_momentum_bps: Bps,
    pub depth_attachment_status: DepthAttachmentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_level_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_imbalance_bps: Option<Bps>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_spread_bps: Option<Bps>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_age_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_missing_reason: Option<DepthMissingReason>,
    pub reason_codes: Vec<ReasonCode>,
}
