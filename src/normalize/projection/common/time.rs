use super::constants::{BOOTSTRAP_ROLLUP_DAYS, ONE_DAY_MS};
use crate::normalize::args::InputRange;
use chrono::{DateTime, Utc};

pub fn bootstrap_rollup_day_starts(input_range: InputRange) -> Vec<i64> {
    let end_day_start_ms = day_start_ms(input_range.end_ms.saturating_sub(1));
    (0..BOOTSTRAP_ROLLUP_DAYS)
        .rev()
        .map(|offset| end_day_start_ms.saturating_sub(offset * ONE_DAY_MS))
        .collect()
}

pub(in crate::normalize::projection) fn day_start_ms(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(ONE_DAY_MS) * ONE_DAY_MS
}

pub(in crate::normalize::projection) fn event_date(day_start_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(day_start_ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .format("%Y-%m-%d")
        .to_string()
}
