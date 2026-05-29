use super::native::assign_native_symbols;
use crate::normalize::model::{SliceRow, SymbolUniverseBootstrapSymbolStats};
use crate::normalize::projection::common::{median, price};

#[derive(Debug, Clone)]
pub(in crate::normalize::projection::symbol_universe) struct BootstrapRunSymbolAccumulator {
    pub(in crate::normalize::projection::symbol_universe) symbol_canonical: String,
    pub(in crate::normalize::projection::symbol_universe) execution_symbol_native: Option<String>,
    pub(in crate::normalize::projection::symbol_universe) reference_symbol_native: Option<String>,
    pub(in crate::normalize::projection::symbol_universe) traded_notional_sum: f64,
    pub(in crate::normalize::projection::symbol_universe) spread_samples: Vec<f64>,
    pub(in crate::normalize::projection::symbol_universe) gap_count: i64,
    pub(in crate::normalize::projection::symbol_universe) window_count: i64,
    pub(in crate::normalize::projection::symbol_universe) mapping_confidence: String,
}

impl BootstrapRunSymbolAccumulator {
    pub(in crate::normalize::projection::symbol_universe) fn observe_slice(
        &mut self,
        row: &SliceRow,
    ) {
        assign_native_symbols(
            row,
            &mut self.execution_symbol_native,
            &mut self.reference_symbol_native,
        );
        let traded_notional = price(row).unwrap_or(0.0) * row.trade_volume;
        if traded_notional.is_finite() && traded_notional > 0.0 {
            self.traded_notional_sum += traded_notional;
        }
        if let Some(spread_bps) = row.spread_bps.filter(|value| value.is_finite()) {
            self.spread_samples.push(spread_bps);
        }
        self.gap_count += row.quality_gap;
        self.window_count += 1;
    }

    pub(in crate::normalize::projection::symbol_universe) fn into_symbol_stats(
        self,
    ) -> SymbolUniverseBootstrapSymbolStats {
        SymbolUniverseBootstrapSymbolStats {
            symbol_canonical: self.symbol_canonical,
            execution_symbol_native: self.execution_symbol_native,
            reference_symbol_native: self.reference_symbol_native,
            traded_notional_sum: self.traded_notional_sum,
            spread_bps_median_samples: median(self.spread_samples).into_iter().collect(),
            gap_count: self.gap_count,
            window_count: self.window_count,
            mapping_confidence: self.mapping_confidence,
        }
    }
}
