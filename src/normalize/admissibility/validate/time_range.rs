use crate::normalize::args::InputRange;

pub(super) type TimeRangeMismatch = (i64, i64, i64, i64);

pub(super) fn validate_time_range(
    actual_start_ms: i64,
    actual_end_ms: i64,
    expected: InputRange,
) -> Result<(), TimeRangeMismatch> {
    if actual_start_ms <= expected.start_ms && actual_end_ms >= expected.end_ms {
        Ok(())
    } else {
        Err((
            actual_start_ms,
            actual_end_ms,
            expected.start_ms,
            expected.end_ms,
        ))
    }
}
