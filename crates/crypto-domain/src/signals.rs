use crate::{
    Bps, Direction, ExpertId, Notional, OrderStyle, ReasonCode, Regime, Symbol, TimestampMs,
    TraceId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegimeSnapshot {
    pub decision_trace_id: TraceId,
    pub symbol: Symbol,
    pub primary_regime: Regime,
    pub confidence_ppm: u32,
    pub valid_until_ms: TimestampMs,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignalOpinion {
    pub decision_trace_id: TraceId,
    pub expert_id: ExpertId,
    pub feature_family: String,
    pub symbol: Symbol,
    pub direction: Direction,
    pub confidence_ppm: u32,
    pub expected_edge_bps: Bps,
    pub horizon_ms: i64,
    pub required_regime: Regime,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TradeCandidate {
    pub decision_trace_id: TraceId,
    pub symbol: Symbol,
    pub source_expert_ids: Vec<ExpertId>,
    pub direction: Direction,
    pub gross_edge_bps: Bps,
    pub confidence_ppm: u32,
    pub horizon_ms: i64,
    pub suggested_notional: Notional,
    pub order_style: OrderStyle,
    pub created_at_ms: TimestampMs,
    pub reason_codes: Vec<ReasonCode>,
}
