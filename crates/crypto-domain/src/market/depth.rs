use crate::{
    Bps, DomainError, EventQuality, ExchangeId, Price, Quantity, Symbol, TimestampMs, TraceId,
};
use serde::{Deserialize, Serialize};

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
    pub sequence: crate::Sequence,
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
