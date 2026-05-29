use crate::normalize::args::InputRange;
use crate::normalize::model::{
    MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION, MarketFeatureDelta, MarketFeatureDeltaSummary,
};
use crate::normalize::projection::common::stable_id;

mod accumulator;
mod grouping;
mod key;
mod newer;

use accumulator::MarketFeatureDeltaSummaryAccumulator;
use grouping::latest_metrics_by_summary_key;

pub fn build_market_feature_delta_summary(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    detail_feature_delta_key: &str,
    deltas: &[MarketFeatureDelta],
) -> MarketFeatureDeltaSummary {
    let rows = latest_metrics_by_summary_key(deltas)
        .into_iter()
        .map(|(key, metrics)| {
            let mut accumulator =
                MarketFeatureDeltaSummaryAccumulator::new(input_range, known_as_of_ms);
            let metrics = metrics
                .into_values()
                .map(|delta| accumulator.observe(delta))
                .collect::<Vec<_>>();
            accumulator.into_row(key, metrics)
        })
        .collect::<Vec<_>>();

    MarketFeatureDeltaSummary {
        schema_version: MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION.to_owned(),
        feature_delta_summary_id: stable_id(&[
            l1_run_id,
            &input_range.start_ms.to_string(),
            &input_range.end_ms.to_string(),
            detail_feature_delta_key,
            MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        detail_feature_delta_key: detail_feature_delta_key.to_owned(),
        window_start_ms: input_range.start_ms,
        window_end_ms: input_range.end_ms,
        known_as_of_ms,
        detail_record_count: deltas.len(),
        summary_row_count: rows.len(),
        rows,
    }
}
