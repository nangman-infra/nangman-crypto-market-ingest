use crate::normalize::args::InputRange;
use crate::normalize::model::{
    MarketFeatureDelta, MarketFeatureDeltaSummaryMetric, MarketFeatureDeltaSummaryRow,
};
use std::collections::BTreeSet;

use super::key::MarketFeatureDeltaSummaryKey;

pub(super) struct MarketFeatureDeltaSummaryAccumulator {
    input_range: InputRange,
    fallback_known_as_of_ms: i64,
    window_start_ms: i64,
    window_end_ms: i64,
    row_known_as_of_ms: i64,
    missing_reasons: BTreeSet<String>,
    has_complete: bool,
    has_partial: bool,
}

impl MarketFeatureDeltaSummaryAccumulator {
    pub(super) fn new(input_range: InputRange, fallback_known_as_of_ms: i64) -> Self {
        Self {
            input_range,
            fallback_known_as_of_ms,
            window_start_ms: i64::MAX,
            window_end_ms: i64::MIN,
            row_known_as_of_ms: i64::MIN,
            missing_reasons: BTreeSet::new(),
            has_complete: false,
            has_partial: false,
        }
    }

    pub(super) fn observe(
        &mut self,
        delta: &MarketFeatureDelta,
    ) -> MarketFeatureDeltaSummaryMetric {
        self.window_start_ms = self.window_start_ms.min(delta.window_start_ms);
        self.window_end_ms = self.window_end_ms.max(delta.window_end_ms);
        self.row_known_as_of_ms = self.row_known_as_of_ms.max(delta.known_as_of_ms);
        self.has_complete |= delta.quality_status == "complete";
        self.has_partial |= delta.quality_status == "partial";
        self.missing_reasons
            .extend(delta.missing_reasons.iter().cloned());
        MarketFeatureDeltaSummaryMetric {
            metric_name: delta.metric_name.clone(),
            value_now: delta.value_now,
            value_15m_ago: delta.value_15m_ago,
            value_1h_ago: delta.value_1h_ago,
            change_pct_15m: delta.change_pct_15m,
            change_pct_1h: delta.change_pct_1h,
            price_change_same_window: delta.price_change_same_window,
            volume_change_same_window: delta.volume_change_same_window,
            oi_price_divergence: delta.oi_price_divergence,
            window_start_ms: delta.window_start_ms,
            window_end_ms: delta.window_end_ms,
            quality_status: delta.quality_status.clone(),
        }
    }

    pub(super) fn into_row(
        self,
        key: MarketFeatureDeltaSummaryKey,
        metrics: Vec<MarketFeatureDeltaSummaryMetric>,
    ) -> MarketFeatureDeltaSummaryRow {
        MarketFeatureDeltaSummaryRow {
            venue: key.venue,
            symbol_native: key.symbol_native,
            symbol_canonical: key.symbol_canonical,
            market_type: key.market_type,
            window_start_ms: self.summary_window_start_ms(),
            window_end_ms: self.summary_window_end_ms(),
            known_as_of_ms: self.summary_known_as_of_ms(),
            quality_status: self.quality_status().to_owned(),
            missing_reasons: self.missing_reasons.into_iter().collect(),
            metrics,
        }
    }

    fn summary_window_start_ms(&self) -> i64 {
        if self.window_start_ms == i64::MAX {
            self.input_range.start_ms
        } else {
            self.window_start_ms
        }
    }

    fn summary_window_end_ms(&self) -> i64 {
        if self.window_end_ms == i64::MIN {
            self.input_range.end_ms
        } else {
            self.window_end_ms
        }
    }

    fn summary_known_as_of_ms(&self) -> i64 {
        if self.row_known_as_of_ms == i64::MIN {
            self.fallback_known_as_of_ms
        } else {
            self.row_known_as_of_ms
        }
    }

    fn quality_status(&self) -> &'static str {
        if self.has_complete {
            "complete"
        } else if self.has_partial {
            "partial"
        } else {
            "insufficient"
        }
    }
}
