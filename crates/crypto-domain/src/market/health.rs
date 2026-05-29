use crate::{ExchangeId, ReasonCode, Symbol, TimestampMs};
use serde::{Deserialize, Serialize};

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
