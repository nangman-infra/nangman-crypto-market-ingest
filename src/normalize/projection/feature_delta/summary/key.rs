use crate::normalize::model::MarketFeatureDelta;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct MarketFeatureDeltaSummaryKey {
    pub(super) venue: String,
    pub(super) symbol_native: String,
    pub(super) symbol_canonical: String,
    pub(super) market_type: String,
}

impl MarketFeatureDeltaSummaryKey {
    pub(super) fn from_delta(delta: &MarketFeatureDelta) -> Self {
        Self {
            venue: delta.venue.clone(),
            symbol_native: delta.symbol_native.clone(),
            symbol_canonical: delta.symbol_canonical.clone(),
            market_type: delta.market_type.clone(),
        }
    }
}
