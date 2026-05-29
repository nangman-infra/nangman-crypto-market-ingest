mod constants;
mod grouping;
mod identity;
mod stats;
mod time;

pub(in crate::normalize::projection) use constants::{
    BOOTSTRAP_ROLLUP_DAYS, DATA_QUALITY_CUTOFF_VERSION, FIFTEEN_MINUTES_MS, MAX_APPROVED_RANK,
    MAX_GAP_RATE, MAX_MEDIAN_SPREAD_BPS, MIN_BOOTSTRAP_DAYS, MIN_REFERENCE_WARMUP_BOOTSTRAP_DAYS,
    ONE_DAY_MS, ONE_HOUR_MS, SELECTION_POLICY_VERSION, VENUE_TRUTH_POLICY_VERSION,
};
pub(in crate::normalize::projection) use grouping::{
    group_slices_by_symbol, price, value_at_or_before, volume,
};
pub(in crate::normalize::projection) use identity::stable_id;
pub(in crate::normalize::projection) use stats::{
    correlation, mean, median, percent_change, population_stddev,
};
pub use time::bootstrap_rollup_day_starts;
pub(in crate::normalize::projection) use time::{day_start_ms, event_date};
