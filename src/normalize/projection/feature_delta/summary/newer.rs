use crate::normalize::model::MarketFeatureDelta;
use std::cmp::Ordering;

pub(super) fn is_newer_delta(
    candidate: &MarketFeatureDelta,
    existing: &MarketFeatureDelta,
) -> bool {
    candidate
        .window_end_ms
        .cmp(&existing.window_end_ms)
        .then_with(|| candidate.window_start_ms.cmp(&existing.window_start_ms))
        .then_with(|| candidate.known_as_of_ms.cmp(&existing.known_as_of_ms))
        == Ordering::Greater
}
