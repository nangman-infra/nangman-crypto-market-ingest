use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

pub type TimestampMs = i64;
pub type Sequence = u64;
pub type TraceId = u128;
pub type RecordId = u64;
pub type ExpertId = String;
pub type ExchangeId = String;
pub type AssetCode = String;

mod enums;
mod numeric;
mod position_payload;
pub use enums::{
    CostDecisionKind, DepthAttachmentStatus, DepthMissingReason, Direction, EventQuality,
    ExecutionDecisionKind, ExpertStatus, FillQuality, LedgerRecordType, OrderStyle, ReasonCode,
    Regime, RiskDecisionKind,
};
pub use numeric::{Bps, FixedDecimal, MicroBps, Notional, Price, Quantity, Ratio};
pub use position_payload::{
    AccountingMethod, PaperPositionAttribution, PaperPositionFee, PaperPositionFill,
    PaperPositionState, PaperPositionUpdatePayload, PaperRealizedPnl, PaperUnrealizedPnl,
    PositionEffect, PositionEventType,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    EmptyDecimal,
    InvalidDecimal(String),
    NegativeUnsignedDecimal(String),
    ScaleOverflow,
    DivideByZero,
    InvalidSymbol(String),
    InvalidMarketValue(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDecimal => write!(f, "decimal value is empty"),
            Self::InvalidDecimal(value) => write!(f, "invalid decimal value: {value}"),
            Self::NegativeUnsignedDecimal(value) => {
                write!(f, "unsigned decimal cannot be negative: {value}")
            }
            Self::ScaleOverflow => write!(f, "decimal scale overflow"),
            Self::DivideByZero => write!(f, "cannot divide by zero"),
            Self::InvalidSymbol(value) => write!(f, "invalid symbol: {value}"),
            Self::InvalidMarketValue(value) => write!(f, "invalid market value: {value}"),
        }
    }
}

impl std::error::Error for DomainError {}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub start_ms: TimestampMs,
    pub end_ms: TimestampMs,
}

impl TimeWindow {
    pub const fn new(start_ms: TimestampMs, end_ms: TimestampMs) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.start_ms >= self.end_ms {
            return Err(DomainError::InvalidMarketValue(
                "time window start must be before end".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Symbol {
    pub exchange: ExchangeId,
    pub base: AssetCode,
    pub quote: AssetCode,
    pub normalized: String,
    pub raw: String,
}

impl Symbol {
    pub fn new(exchange: &str, base: &str, quote: &str, raw: &str) -> Result<Self, DomainError> {
        if exchange.trim().is_empty()
            || base.trim().is_empty()
            || quote.trim().is_empty()
            || raw.trim().is_empty()
        {
            return Err(DomainError::InvalidSymbol(format!(
                "{exchange}:{base}/{quote}:{raw}"
            )));
        }
        let base = base.trim().to_ascii_uppercase();
        let quote = quote.trim().to_ascii_uppercase();
        Ok(Self {
            exchange: exchange.trim().to_owned(),
            normalized: format!("{base}-{quote}"),
            base,
            quote,
            raw: raw.trim().to_owned(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketSnapshot {
    pub decision_trace_id: TraceId,
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub event_time_ms: TimestampMs,
    pub received_time_ms: TimestampMs,
    pub sequence: Sequence,
    pub quality: EventQuality,
    pub last_price: Price,
    pub best_bid: Price,
    pub best_ask: Price,
    pub best_bid_qty: Quantity,
    pub best_ask_qty: Quantity,
    pub spread_bps: Bps,
}

impl MarketSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.decision_trace_id == 0 {
            return Err(DomainError::InvalidMarketValue(
                "trace id is zero".to_owned(),
            ));
        }
        if self.event_time_ms <= 0 || self.received_time_ms <= 0 {
            return Err(DomainError::InvalidMarketValue(
                "timestamp must be positive".to_owned(),
            ));
        }
        if !self.last_price.is_positive()
            || !self.best_bid.is_positive()
            || !self.best_ask.is_positive()
            || !self.best_bid_qty.is_non_negative()
            || !self.best_ask_qty.is_non_negative()
        {
            return Err(DomainError::InvalidMarketValue(
                "price and quantity constraints failed".to_owned(),
            ));
        }
        if self.best_bid.checked_gt(self.best_ask)? {
            return Err(DomainError::InvalidMarketValue(
                "best bid cannot be greater than best ask".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossSectionalMarketFrame {
    pub decision_trace_id: TraceId,
    pub exchange: ExchangeId,
    pub event_time_ms: TimestampMs,
    pub received_time_ms: TimestampMs,
    pub sequence: Sequence,
    pub markets: Vec<MarketSnapshot>,
}

impl CrossSectionalMarketFrame {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.decision_trace_id == 0 {
            return Err(DomainError::InvalidMarketValue(
                "cross-sectional trace id is zero".to_owned(),
            ));
        }
        if self.event_time_ms <= 0 || self.received_time_ms <= 0 {
            return Err(DomainError::InvalidMarketValue(
                "cross-sectional timestamp must be positive".to_owned(),
            ));
        }
        if self.markets.len() < 2 {
            return Err(DomainError::InvalidMarketValue(
                "cross-sectional frame requires at least two markets".to_owned(),
            ));
        }

        let mut symbols = HashSet::new();
        for market in &self.markets {
            market.validate()?;
            if market.exchange != self.exchange {
                return Err(DomainError::InvalidMarketValue(
                    "cross-sectional frame requires one exchange".to_owned(),
                ));
            }
            if !symbols.insert(market.symbol.normalized.as_str()) {
                return Err(DomainError::InvalidMarketValue(format!(
                    "duplicate symbol in cross-sectional frame: {}",
                    market.symbol.normalized
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderBookLevel {
    pub price: Price,
    pub quantity: Quantity,
}

impl OrderBookLevel {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !self.price.is_positive() || !self.quantity.is_non_negative() {
            return Err(DomainError::InvalidMarketValue(
                "order book level price and quantity constraints failed".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDepthSnapshot {
    pub decision_trace_id: TraceId,
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub event_time_ms: TimestampMs,
    pub received_time_ms: TimestampMs,
    pub sequence: Sequence,
    pub quality: EventQuality,
    pub level_count: usize,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub bid_depth_qty: Quantity,
    pub ask_depth_qty: Quantity,
    pub depth_imbalance_bps: Bps,
    pub spread_bps: Bps,
}

impl MarketDepthSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.decision_trace_id == 0 {
            return Err(DomainError::InvalidMarketValue(
                "trace id is zero".to_owned(),
            ));
        }
        if self.event_time_ms <= 0 || self.received_time_ms <= 0 {
            return Err(DomainError::InvalidMarketValue(
                "timestamp must be positive".to_owned(),
            ));
        }
        if self.level_count == 0 || self.bids.is_empty() || self.asks.is_empty() {
            return Err(DomainError::InvalidMarketValue(
                "market depth requires at least one bid and ask level".to_owned(),
            ));
        }
        for level in self.bids.iter().chain(self.asks.iter()) {
            level.validate()?;
        }
        if !self.bid_depth_qty.is_non_negative() || !self.ask_depth_qty.is_non_negative() {
            return Err(DomainError::InvalidMarketValue(
                "depth quantity constraints failed".to_owned(),
            ));
        }
        let best_bid = self.bids[0].price;
        let best_ask = self.asks[0].price;
        if best_bid.checked_gt(best_ask)? {
            return Err(DomainError::InvalidMarketValue(
                "depth best bid cannot be greater than best ask".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SymbolHealthSnapshot {
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub last_event_time_ms: TimestampMs,
    pub last_received_time_ms: TimestampMs,
    pub latency_ms: i64,
    pub gap_count: u32,
    pub stale_count: u32,
    pub overflow_count: u32,
    pub is_tradeable: bool,
    pub reason_codes: Vec<ReasonCode>,
}

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
    pub decision: CostDecisionKind,
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

#[cfg(test)]
mod tests;
