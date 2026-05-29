use super::snapshot::MarketSnapshot;
use crate::{DomainError, ExchangeId, TimestampMs, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrossSectionalMarketFrame {
    pub decision_trace_id: TraceId,
    pub exchange: ExchangeId,
    pub event_time_ms: TimestampMs,
    pub received_time_ms: TimestampMs,
    pub sequence: crate::Sequence,
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
