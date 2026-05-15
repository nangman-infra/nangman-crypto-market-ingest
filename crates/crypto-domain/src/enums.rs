use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Long,
    Flat,
    Exit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OrderStyle {
    Maker,
    Taker,
    Reject,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EventQuality {
    Ok,
    Delayed,
    Gap,
    Stale,
    Invalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DepthAttachmentStatus {
    Attached,
    Missing,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DepthMissingReason {
    NoSnapshotSeen,
    SnapshotAfterMarket,
    SnapshotTooOld,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Regime {
    TrendUp,
    TrendDown,
    Range,
    Squeeze,
    Jump,
    ToxicFlow,
    Illiquid,
    Uncertain,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExpertStatus {
    Candidate,
    PaperProbation,
    PaperActive,
    Throttled,
    Retired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReasonCode {
    EventDelayed,
    EventGap,
    EventStale,
    EventInvalid,
    SymbolUnhealthy,
    UnfinishedCandle,
    ChannelOverflow,
    RegimeMismatch,
    LowConfidence,
    RecentFailure,
    NoConsensus,
    DuplicateSignal,
    ExpertDisabled,
    CostTooHigh,
    LatencyTooHigh,
    SpreadTooWide,
    AdversePriceImpact,
    #[serde(rename = "ADVERSE_PRICE_IMPACT_10_TICK")]
    AdversePriceImpact10Tick,
    AdverseOrderbookImbalance,
    AdverseDepthImbalance,
    AdverseMicropriceEdge,
    WeakRelativeMomentum,
    InsufficientTradeIntensityExpansion,
    InsufficientVolatilityExpansion,
    AdverseMicrostructure,
    SlippageUnknown,
    FillQualityLow,
    RiskLimit,
    DailyStop,
    WeeklyDrawdownReduced,
    MonthlyDrawdownPaperOnly,
    SymbolStop,
    StrategyThrottled,
    CooldownActive,
    NotionalCap,
    PortfolioCap,
    PaperOnly,
    LocalRiskFlag,
    SimulationRejected,
    PartialFillSimulated,
    PairAtomicityRequired,
    PairLeggingRisk,
    PairCompensationRequired,
    LedgerWriteFailed,
    TraceIdMissing,
    CorrectionAppended,
    MarketEventAccepted,
    MetaCandidateCreated,
    CostApproved,
    RiskApproved,
    RiskCapped,
    PaperOrderSimulated,
    PaperFillCreated,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "PascalCase")]
pub enum LedgerRecordType {
    MarketEventAccepted,
    MarketEventRejected,
    FeatureSnapshot,
    RegimeChanged,
    SignalOpinionCreated,
    OpinionRejected,
    TradeCandidateCreated,
    CostApproved,
    CostRejected,
    RiskApproved,
    RiskCapped,
    RiskRejected,
    SymbolBlocked,
    DailyRiskBlocked,
    WeeklyRiskReduced,
    MonthlyPaperOnly,
    PaperOrderSimulated,
    PaperFillCreated,
    PaperFillRejected,
    PaperPositionUpdated,
    CorrectionAppended,
    EngineStopped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CostDecisionKind {
    Approve,
    Reject,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RiskDecisionKind {
    Approve,
    CapSize,
    Reject,
    BlockSymbol,
    BlockAll,
    PaperOnly,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FillQuality {
    Good,
    Partial,
    Poor,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionDecisionKind {
    SimulatedFill,
    PartialFill,
    Rejected,
}
