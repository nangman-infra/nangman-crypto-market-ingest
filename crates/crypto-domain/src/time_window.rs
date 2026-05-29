use crate::{DomainError, TimestampMs};
use serde::{Deserialize, Serialize};

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
