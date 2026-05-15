use std::time::{SystemTime, UNIX_EPOCH};

use crypto_domain::TimestampMs;

use super::MarketDataError;

pub(super) fn now_ms() -> Result<TimestampMs, MarketDataError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| MarketDataError::InvalidMessage(format!("system time error: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| MarketDataError::InvalidMessage("system time overflow".to_owned()))
}
