use crate::{
    Bps, Direction, ExecutionDecisionKind, ExpertId, FillQuality, Notional, OrderStyle, Price,
    Quantity, ReasonCode, RiskDecisionKind, Symbol, TimestampMs, TraceId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CostDecision {
    pub decision_trace_id: TraceId,
    pub symbol: Symbol,
    pub source_expert_ids: Vec<ExpertId>,
    pub direction: Direction,
    pub order_style: OrderStyle,
    pub gross_edge_bps: Bps,
    pub fee_bps: Bps,
    pub spread_cost_bps: Bps,
    pub slippage_bps: Bps,
    pub latency_penalty_bps: Bps,
    pub adverse_selection_bps: Bps,
    pub safety_margin_bps: Bps,
    pub net_edge_bps: Bps,
    pub minimum_required_edge_bps: Bps,
    pub decision: crate::CostDecisionKind,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskDecision {
    pub decision_trace_id: TraceId,
    pub symbol: Symbol,
    pub source_expert_ids: Vec<ExpertId>,
    pub decision: RiskDecisionKind,
    pub requested_notional: Notional,
    pub allowed_notional: Notional,
    pub max_symbol_notional: Notional,
    pub max_total_notional: Notional,
    pub daily_drawdown_bps: Bps,
    pub weekly_drawdown_bps: Bps,
    pub monthly_drawdown_bps: Bps,
    pub cooldown_until_ms: TimestampMs,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionResult {
    pub decision_trace_id: TraceId,
    pub symbol: Symbol,
    pub order_style: OrderStyle,
    pub requested_quantity: Quantity,
    pub filled_quantity: Quantity,
    pub simulated_price: Price,
    pub reference_price: Price,
    pub fee_bps: Bps,
    pub slippage_bps: Bps,
    pub latency_ms: i64,
    pub fill_quality: FillQuality,
    pub decision: ExecutionDecisionKind,
    pub reason_codes: Vec<ReasonCode>,
}
