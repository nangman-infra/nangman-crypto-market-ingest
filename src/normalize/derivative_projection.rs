use super::args::InputRange;
use super::model::{
    DerivativeMetricObservation, MARKET_FEATURE_DELTA_SCHEMA_VERSION, MarketFeatureDelta,
};
use crate::storage::record::sha256_hex;
use std::collections::BTreeMap;

const FIFTEEN_MINUTES_MS: i64 = 900_000;
const ONE_HOUR_MS: i64 = 3_600_000;

pub fn build_derivative_feature_deltas(
    l1_run_id: &str,
    input_range: InputRange,
    projection_derivative_metrics: &[DerivativeMetricObservation],
) -> Vec<MarketFeatureDelta> {
    let grouped = group_derivative_metrics(projection_derivative_metrics);
    let mut deltas = Vec::new();
    for rows in grouped.values() {
        for row in rows
            .iter()
            .copied()
            .filter(|row| row.exchange_timestamp_ms >= input_range.start_ms)
        {
            let value_15m =
                derivative_value_at_or_before(rows, row.exchange_timestamp_ms - FIFTEEN_MINUTES_MS);
            let value_1h =
                derivative_value_at_or_before(rows, row.exchange_timestamp_ms - ONE_HOUR_MS);
            deltas.push(derivative_feature_delta(
                l1_run_id,
                row,
                DerivativeFeatureDeltaValues {
                    value_now: row.value,
                    value_15m_ago: value_15m,
                    value_1h_ago: value_1h,
                    change_pct_15m: percent_change(Some(row.value), value_15m),
                    change_pct_1h: percent_change(Some(row.value), value_1h),
                },
            ));
        }
    }
    deltas
}

fn group_derivative_metrics(
    metrics: &[DerivativeMetricObservation],
) -> BTreeMap<DerivativeMetricKey, Vec<&DerivativeMetricObservation>> {
    let mut grouped = BTreeMap::<DerivativeMetricKey, Vec<&DerivativeMetricObservation>>::new();
    for metric in metrics {
        grouped
            .entry(DerivativeMetricKey {
                venue: metric.venue.clone(),
                symbol_native: metric.symbol_native.clone(),
                symbol_canonical: metric.symbol_canonical.clone(),
                market_type: metric.market_type.clone(),
                metric_name: metric.metric_name.clone(),
            })
            .or_default()
            .push(metric);
    }
    for rows in grouped.values_mut() {
        rows.sort_by_key(|row| row.exchange_timestamp_ms);
    }
    grouped
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DerivativeMetricKey {
    venue: String,
    symbol_native: String,
    symbol_canonical: String,
    market_type: String,
    metric_name: String,
}

fn derivative_feature_delta(
    l1_run_id: &str,
    row: &DerivativeMetricObservation,
    values: DerivativeFeatureDeltaValues,
) -> MarketFeatureDelta {
    let mut missing_reasons = Vec::new();
    if values.value_15m_ago.is_none() {
        missing_reasons.push("value_15m_ago_missing".to_owned());
    }
    if values.value_1h_ago.is_none() {
        missing_reasons.push("value_1h_ago_missing".to_owned());
    }
    let quality_status = if missing_reasons.is_empty() {
        "complete"
    } else if values.change_pct_15m.is_some() || values.change_pct_1h.is_some() {
        "partial"
    } else {
        "insufficient"
    };
    MarketFeatureDelta {
        schema_version: MARKET_FEATURE_DELTA_SCHEMA_VERSION.to_owned(),
        feature_delta_id: stable_id(&[
            l1_run_id,
            row.metric_name.as_str(),
            row.venue.as_str(),
            row.symbol_native.as_str(),
            &row.exchange_timestamp_ms.to_string(),
            MARKET_FEATURE_DELTA_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        metric_name: row.metric_name.clone(),
        venue: row.venue.clone(),
        symbol_native: row.symbol_native.clone(),
        symbol_canonical: row.symbol_canonical.clone(),
        market_type: row.market_type.clone(),
        value_now: values.value_now,
        value_15m_ago: values.value_15m_ago,
        value_1h_ago: values.value_1h_ago,
        change_pct_15m: values.change_pct_15m,
        change_pct_1h: values.change_pct_1h,
        price_change_same_window: None,
        volume_change_same_window: None,
        oi_price_divergence: None,
        window_start_ms: row.exchange_timestamp_ms,
        window_end_ms: row.exchange_timestamp_ms.saturating_add(1_000),
        known_as_of_ms: row.ingest_timestamp_ms,
        quality_status: quality_status.to_owned(),
        missing_reasons,
    }
}

#[derive(Debug, Clone, Copy)]
struct DerivativeFeatureDeltaValues {
    value_now: f64,
    value_15m_ago: Option<f64>,
    value_1h_ago: Option<f64>,
    change_pct_15m: Option<f64>,
    change_pct_1h: Option<f64>,
}

fn derivative_value_at_or_before(
    rows: &[&DerivativeMetricObservation],
    target_exchange_timestamp_ms: i64,
) -> Option<f64> {
    rows.iter()
        .rev()
        .find(|row| row.exchange_timestamp_ms <= target_exchange_timestamp_ms)
        .map(|row| row.value)
        .filter(|value| value.is_finite())
}

fn percent_change(now: Option<f64>, previous: Option<f64>) -> Option<f64> {
    let now = now?;
    let previous = previous?;
    if !now.is_finite() || !previous.is_finite() || previous.abs() <= f64::EPSILON {
        return None;
    }
    Some(((now - previous) / previous) * 100.0)
}

fn stable_id(parts: &[&str]) -> String {
    sha256_hex(parts.join("|").as_bytes())
}
