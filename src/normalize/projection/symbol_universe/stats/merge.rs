use crate::normalize::model::SymbolUniverseBootstrapSymbolStats;

pub(in crate::normalize::projection::symbol_universe) fn merge_symbol_rollup(
    existing: &mut SymbolUniverseBootstrapSymbolStats,
    current: &SymbolUniverseBootstrapSymbolStats,
) {
    if existing.execution_symbol_native.is_none() {
        existing.execution_symbol_native = current.execution_symbol_native.clone();
    }
    if existing.reference_symbol_native.is_none() {
        existing.reference_symbol_native = current.reference_symbol_native.clone();
    }
    existing.traded_notional_sum += current.traded_notional_sum;
    existing
        .spread_bps_median_samples
        .extend(current.spread_bps_median_samples.iter().copied());
    existing.gap_count += current.gap_count;
    existing.window_count += current.window_count;
    if existing.mapping_confidence == "unknown" {
        existing.mapping_confidence = current.mapping_confidence.clone();
    }
}
