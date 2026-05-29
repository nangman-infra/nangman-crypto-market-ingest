use serde_json::Value;

use super::common::number_from_value;
use crate::normalize::model::{DerivativeMetricObservation, RawInputEvent};

pub fn parse_derivative_metric(event: &RawInputEvent) -> Option<DerivativeMetricObservation> {
    let value = serde_json::from_str::<Value>(&event.payload_json).ok()?;
    let (metric_name, metric_value, unit) = match (event.venue.as_str(), event.event_type.as_str())
    {
        ("binance", "funding_rate_snapshot") => (
            "funding_rate",
            number_from_value(
                value
                    .get("funding_rate")
                    .or_else(|| value.get("lastFundingRate"))?,
            )?,
            "ratio",
        ),
        ("binance", "open_interest_snapshot") => (
            "open_interest",
            number_from_value(
                value
                    .get("open_interest")
                    .or_else(|| value.get("openInterest"))?,
            )?,
            "contracts",
        ),
        _ => return None,
    };
    if !metric_value.is_finite() {
        return None;
    }
    Some(DerivativeMetricObservation {
        venue: event.venue.clone(),
        source_role: event.source_role.clone(),
        market_type: event.market_type.clone(),
        metric_name: metric_name.to_owned(),
        symbol_native: event.symbol_native.clone(),
        symbol_canonical: event.symbol_canonical.clone(),
        base_asset: event.base_asset.clone(),
        quote_asset: event.quote_asset.clone(),
        value: metric_value,
        unit: unit.to_owned(),
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        parent_event_id: event.event_id.clone(),
        parent_run_id: event.producer_run_id.clone(),
    })
}
