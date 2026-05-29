pub(in crate::normalize::projection) const SELECTION_POLICY_VERSION: &str =
    "observed_liquidity_rank_p0_v1";
pub(in crate::normalize::projection) const VENUE_TRUTH_POLICY_VERSION: &str =
    "execution_reference_split_p0_v1";
pub(in crate::normalize::projection) const DATA_QUALITY_CUTOFF_VERSION: &str =
    "requires_30d_bootstrap_p0_v1";
pub(in crate::normalize::projection) const FIFTEEN_MINUTES_MS: i64 = 900_000;
pub(in crate::normalize::projection) const ONE_HOUR_MS: i64 = 3_600_000;
pub(in crate::normalize::projection) const ONE_DAY_MS: i64 = 86_400_000;
pub(in crate::normalize::projection) const MIN_BOOTSTRAP_DAYS: i64 = 30;
pub(in crate::normalize::projection) const BOOTSTRAP_ROLLUP_DAYS: i64 = 30;
pub(in crate::normalize::projection) const MAX_APPROVED_RANK: i64 = 50;
pub(in crate::normalize::projection) const MIN_REFERENCE_WARMUP_BOOTSTRAP_DAYS: i64 = 1;
pub(in crate::normalize::projection) const MAX_MEDIAN_SPREAD_BPS: f64 = 50.0;
pub(in crate::normalize::projection) const MAX_GAP_RATE: f64 = 0.05;
