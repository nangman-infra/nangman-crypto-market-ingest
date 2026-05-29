#[derive(Debug, Clone, Copy)]
pub(super) struct MarketFeatureDeltaValues {
    pub(super) value_now: f64,
    pub(super) value_15m_ago: Option<f64>,
    pub(super) value_1h_ago: Option<f64>,
    pub(super) change_pct_15m: Option<f64>,
    pub(super) change_pct_1h: Option<f64>,
    pub(super) price_change_same_window: Option<f64>,
    pub(super) volume_change_same_window: Option<f64>,
    pub(super) oi_price_divergence: Option<f64>,
    pub(super) known_as_of_ms: i64,
}
