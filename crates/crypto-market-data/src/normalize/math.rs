use crate::MAX_CLOCK_SKEW_MS;
use crate::error::MarketDataError;
use crypto_domain::{Bps, DomainError, EventQuality, FixedDecimal, TimestampMs};

pub(super) fn calculate_depth_imbalance_bps(
    bid_depth_qty: FixedDecimal,
    ask_depth_qty: FixedDecimal,
) -> Result<Bps, MarketDataError> {
    let total_qty = bid_depth_qty.checked_add(ask_depth_qty)?;
    if total_qty.value == 0 {
        return Ok(Bps::new(0));
    }
    let delta_qty = bid_depth_qty.checked_sub(ask_depth_qty)?;
    let ratio = delta_qty.div_to_scale(total_qty, 8)?;
    let value = ratio
        .value
        .checked_mul(10_000)
        .ok_or(DomainError::ScaleOverflow)?
        / 100_000_000_i128;
    let value = i64::try_from(value).map_err(|_| DomainError::ScaleOverflow)?;
    Ok(Bps::new(value))
}

pub(super) fn classify_quality(
    event_time_ms: TimestampMs,
    received_time_ms: TimestampMs,
    max_latency_ms: i64,
) -> EventQuality {
    if event_time_ms <= 0 || received_time_ms <= 0 || received_time_ms < event_time_ms {
        return EventQuality::Invalid;
    }
    if received_time_ms - event_time_ms > max_latency_ms {
        return EventQuality::Delayed;
    }
    EventQuality::Ok
}

pub(super) fn normalize_received_time(
    event_time_ms: TimestampMs,
    received_time_ms: TimestampMs,
) -> TimestampMs {
    if event_time_ms > 0
        && received_time_ms > 0
        && received_time_ms < event_time_ms
        && event_time_ms - received_time_ms <= MAX_CLOCK_SKEW_MS
    {
        event_time_ms
    } else {
        received_time_ms
    }
}

pub(super) fn midpoint_price(
    best_bid: FixedDecimal,
    best_ask: FixedDecimal,
) -> Result<FixedDecimal, MarketDataError> {
    let total = best_bid.checked_add(best_ask)?;
    Ok(FixedDecimal::new(total.value / 2, total.scale))
}

pub(super) fn calculate_spread_bps(
    best_bid: FixedDecimal,
    best_ask: FixedDecimal,
    reference_price: FixedDecimal,
) -> Result<Bps, MarketDataError> {
    if best_ask.checked_lt(best_bid)? {
        return Ok(Bps::new(0));
    }
    let spread = best_ask.checked_sub(best_bid)?;
    let spread_ratio = spread.div_to_scale(reference_price, 8)?;
    let numerator = spread_ratio
        .value
        .checked_mul(10_000)
        .ok_or(DomainError::ScaleOverflow)?
        .max(0);
    let denominator = 100_000_000_i128;
    let value = if numerator == 0 {
        0
    } else {
        (numerator + denominator - 1) / denominator
    };
    let value = i64::try_from(value).map_err(|_| DomainError::ScaleOverflow)?;
    Ok(Bps::new(value))
}
