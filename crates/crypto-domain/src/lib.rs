pub type TimestampMs = i64;
pub type Sequence = u64;
pub type TraceId = u128;
pub type RecordId = u64;
pub type ExpertId = String;
pub type ExchangeId = String;
pub type AssetCode = String;

mod ai;
mod decisions;
mod enums;
mod error;
mod market;
mod numeric;
mod position_payload;
mod signals;
mod symbol;
mod time_window;

pub use ai::{
    CandidateDecisionLabel, CandidateFeatureVector, CandidatePerformanceVector,
    LIGHTWEIGHT_AI_CANDIDATE_RANKER_SCHEMA_VERSION, LightweightAiTrainingRow,
};
pub use decisions::{CostDecision, ExecutionResult, RiskDecision};
pub use enums::{
    CostDecisionKind, DepthAttachmentStatus, DepthMissingReason, Direction, EventQuality,
    ExecutionDecisionKind, ExpertStatus, FillQuality, LedgerRecordType, OrderStyle, ReasonCode,
    Regime, RiskDecisionKind,
};
pub use error::DomainError;
pub use market::{
    CrossSectionalMarketFrame, MarketDepthSnapshot, MarketSnapshot, OrderBookLevel,
    RollingFeatures, SymbolHealthSnapshot,
};
pub use numeric::{Bps, FixedDecimal, MicroBps, Notional, Price, Quantity, Ratio};
pub use position_payload::{
    AccountingMethod, PaperPositionAttribution, PaperPositionFee, PaperPositionFill,
    PaperPositionState, PaperPositionUpdatePayload, PaperRealizedPnl, PaperUnrealizedPnl,
    PositionEffect, PositionEventType,
};
pub use signals::{RegimeSnapshot, SignalOpinion, TradeCandidate};
pub use symbol::Symbol;
pub use time_window::TimeWindow;

#[cfg(test)]
mod tests;
