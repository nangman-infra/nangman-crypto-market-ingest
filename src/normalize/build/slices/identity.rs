use crate::normalize::model::RawInputEvent;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::normalize::build) struct SliceKey {
    pub(in crate::normalize::build::slices) venue: String,
    pub(in crate::normalize::build::slices) symbol_canonical: String,
    pub(in crate::normalize::build::slices) window_start_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::normalize::build) struct IdentityKey {
    venue: String,
    symbol_canonical: String,
}

impl IdentityKey {
    pub(in crate::normalize::build) fn from_event(event: &RawInputEvent) -> Self {
        Self {
            venue: event.venue.clone(),
            symbol_canonical: event.symbol_canonical.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::normalize::build) struct Identity {
    pub(in crate::normalize::build::slices) venue: String,
    pub(in crate::normalize::build::slices) source_role: String,
    pub(in crate::normalize::build::slices) symbol_native: String,
    pub(in crate::normalize::build::slices) symbol_canonical: String,
    pub(in crate::normalize::build::slices) base_asset: String,
    pub(in crate::normalize::build::slices) quote_asset: String,
    pub(in crate::normalize::build::slices) market_type: String,
}

impl Identity {
    pub(in crate::normalize::build) fn from_event(event: &RawInputEvent) -> Self {
        Self {
            venue: event.venue.clone(),
            source_role: event.source_role.clone(),
            symbol_native: event.symbol_native.clone(),
            symbol_canonical: event.symbol_canonical.clone(),
            base_asset: event.base_asset.clone(),
            quote_asset: event.quote_asset.clone(),
            market_type: event.market_type.clone(),
        }
    }
}
