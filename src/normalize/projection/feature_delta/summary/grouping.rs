use crate::normalize::model::MarketFeatureDelta;
use std::collections::BTreeMap;

use super::key::MarketFeatureDeltaSummaryKey;
use super::newer::is_newer_delta;

pub(super) fn latest_metrics_by_summary_key(
    deltas: &[MarketFeatureDelta],
) -> BTreeMap<MarketFeatureDeltaSummaryKey, BTreeMap<String, &MarketFeatureDelta>> {
    let mut grouped =
        BTreeMap::<MarketFeatureDeltaSummaryKey, BTreeMap<String, &MarketFeatureDelta>>::new();
    for delta in deltas {
        let metrics = grouped
            .entry(MarketFeatureDeltaSummaryKey::from_delta(delta))
            .or_default();
        match metrics.get(&delta.metric_name) {
            Some(existing) if !is_newer_delta(delta, existing) => {}
            _ => {
                metrics.insert(delta.metric_name.clone(), delta);
            }
        }
    }
    grouped
}
