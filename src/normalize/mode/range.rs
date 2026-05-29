use std::error::Error;

pub(super) fn align_floor(value: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return value;
    }
    let remainder = value.rem_euclid(interval);
    value.saturating_sub(remainder)
}

pub(super) fn validate_range(
    start_ms: i64,
    end_ms: i64,
    window_ms: i64,
    schedule_interval_ms: i64,
) -> Result<(), Box<dyn Error>> {
    if start_ms < 0 || end_ms <= start_ms {
        return Err("input range must be positive and non-empty".into());
    }
    if window_ms > 0 && (start_ms % window_ms != 0 || end_ms % window_ms != 0) {
        return Err("input range must align to window_ms".into());
    }
    if schedule_interval_ms > 0
        && (start_ms % schedule_interval_ms != 0 || end_ms % schedule_interval_ms != 0)
    {
        return Err("input range must align to schedule_interval_ms".into());
    }
    Ok(())
}
