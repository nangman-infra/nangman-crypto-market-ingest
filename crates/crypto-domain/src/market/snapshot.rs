use crate::{
    Bps, DomainError, EventQuality, ExchangeId, Price, Quantity, Symbol, TimestampMs, TraceId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketSnapshot {
    pub decision_trace_id: TraceId,
    pub exchange: ExchangeId,
    pub symbol: Symbol,
    pub event_time_ms: TimestampMs,
    pub received_time_ms: TimestampMs,
    pub sequence: crate::Sequence,
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
