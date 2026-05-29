use super::types::MarketFeatureDeltaValues;
use crate::normalize::args::InputRange;
use crate::normalize::derivative_projection::build_derivative_feature_deltas;
use crate::normalize::model::{
    DerivativeMetricObservation, MARKET_FEATURE_DELTA_SCHEMA_VERSION, MarketFeatureDelta, SliceRow,
};
use crate::normalize::projection::common::{
    FIFTEEN_MINUTES_MS, ONE_HOUR_MS, group_slices_by_symbol, percent_change, price, stable_id,
    value_at_or_before, volume,
};

pub fn build_market_feature_deltas(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    projection_slices: &[SliceRow],
    projection_derivative_metrics: &[DerivativeMetricObservation],
) -> Vec<MarketFeatureDelta> {
    let grouped = group_slices_by_symbol(projection_slices);
    let mut deltas = Vec::new();
    for rows in grouped.values() {
        for row in rows
            .iter()
            .copied()
            .filter(|row| row.window_start_ms >= input_range.start_ms)
        {
            let price_now = price(row);
            let volume_now = Some(row.trade_volume);
            let price_15m =
                value_at_or_before(rows, row.window_start_ms - FIFTEEN_MINUTES_MS, price);
            let price_1h = value_at_or_before(rows, row.window_start_ms - ONE_HOUR_MS, price);
            let volume_15m =
                value_at_or_before(rows, row.window_start_ms - FIFTEEN_MINUTES_MS, volume);
            let volume_1h = value_at_or_before(rows, row.window_start_ms - ONE_HOUR_MS, volume);
            let price_change_15m = percent_change(price_now, price_15m);
            let price_change_1h = percent_change(price_now, price_1h);
            let volume_change_15m = percent_change(volume_now, volume_15m);
            let volume_change_1h = percent_change(volume_now, volume_1h);

            if let Some(value_now) = price_now {
                deltas.push(market_feature_delta(
                    l1_run_id,
                    "price",
                    row,
                    MarketFeatureDeltaValues {
                        value_now,
                        value_15m_ago: price_15m,
                        value_1h_ago: price_1h,
                        change_pct_15m: price_change_15m,
                        change_pct_1h: price_change_1h,
                        price_change_same_window: price_change_1h,
                        volume_change_same_window: volume_change_1h,
                        oi_price_divergence: None,
                        known_as_of_ms,
                    },
                ));
            }

            deltas.push(market_feature_delta(
                l1_run_id,
                "trade_volume",
                row,
                MarketFeatureDeltaValues {
                    value_now: row.trade_volume,
                    value_15m_ago: volume_15m,
                    value_1h_ago: volume_1h,
                    change_pct_15m: volume_change_15m,
                    change_pct_1h: volume_change_1h,
                    price_change_same_window: price_change_1h,
                    volume_change_same_window: volume_change_1h,
                    oi_price_divergence: None,
                    known_as_of_ms,
                },
            ));
        }
    }
    deltas.extend(build_derivative_feature_deltas(
        l1_run_id,
        input_range,
        projection_derivative_metrics,
    ));
    deltas.sort_by(|left, right| {
        left.window_start_ms
            .cmp(&right.window_start_ms)
            .then_with(|| left.venue.cmp(&right.venue))
            .then_with(|| left.symbol_canonical.cmp(&right.symbol_canonical))
            .then_with(|| left.metric_name.cmp(&right.metric_name))
    });
    deltas
}

fn market_feature_delta(
    l1_run_id: &str,
    metric_name: &str,
    row: &SliceRow,
    values: MarketFeatureDeltaValues,
) -> MarketFeatureDelta {
    let mut missing_reasons = Vec::new();
    if values.value_15m_ago.is_none() {
        missing_reasons.push("value_15m_ago_missing".to_owned());
    }
    if values.value_1h_ago.is_none() {
        missing_reasons.push("value_1h_ago_missing".to_owned());
    }
    if values.price_change_same_window.is_none() {
        missing_reasons.push("price_change_same_window_missing".to_owned());
    }
    if values.volume_change_same_window.is_none() {
        missing_reasons.push("volume_change_same_window_missing".to_owned());
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
            metric_name,
            row.venue.as_str(),
            row.symbol_native.as_str(),
            &row.window_start_ms.to_string(),
            MARKET_FEATURE_DELTA_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        metric_name: metric_name.to_owned(),
        venue: row.venue.clone(),
        symbol_native: row.symbol_native.clone(),
        symbol_canonical: row.symbol_canonical.clone(),
        market_type: row.market_type.clone(),
        value_now: values.value_now,
        value_15m_ago: values.value_15m_ago,
        value_1h_ago: values.value_1h_ago,
        change_pct_15m: values.change_pct_15m,
        change_pct_1h: values.change_pct_1h,
        price_change_same_window: values.price_change_same_window,
        volume_change_same_window: values.volume_change_same_window,
        oi_price_divergence: values.oi_price_divergence,
        window_start_ms: row.window_start_ms,
        window_end_ms: row.window_end_ms,
        known_as_of_ms: values.known_as_of_ms,
        quality_status: quality_status.to_owned(),
        missing_reasons,
    }
}
