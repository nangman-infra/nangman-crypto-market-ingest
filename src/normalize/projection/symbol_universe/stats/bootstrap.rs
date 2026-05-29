use super::current::symbol_stats;
use super::types::SymbolStats;
use crate::normalize::args::InputRange;
use crate::normalize::model::{
    SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION, SliceRow, SymbolUniverseBootstrapRollup,
    SymbolUniverseBootstrapSymbolStats,
};
use crate::normalize::projection::common::{
    BOOTSTRAP_ROLLUP_DAYS, ONE_DAY_MS, day_start_ms, median,
};
use std::collections::BTreeMap;

pub(in crate::normalize::projection::symbol_universe) fn symbol_stats_from_bootstrap(
    input_range: InputRange,
    current_slices: &[SliceRow],
    bootstrap_rollups: &[SymbolUniverseBootstrapRollup],
) -> BTreeMap<String, SymbolStats> {
    let mut stats = symbol_stats(current_slices);
    let bootstrap_start_day_ms = day_start_ms(
        input_range
            .end_ms
            .saturating_sub(1)
            .saturating_sub((BOOTSTRAP_ROLLUP_DAYS - 1) * ONE_DAY_MS),
    );
    let bootstrap_end_day_ms = day_start_ms(input_range.end_ms.saturating_sub(1));
    let mut daily_notional = BTreeMap::<String, BTreeMap<i64, f64>>::new();
    let mut spread_samples = BTreeMap::<String, Vec<f64>>::new();
    let mut gap_counts = BTreeMap::<String, (i64, i64)>::new();

    for rollup in bootstrap_rollups.iter().filter(|rollup| {
        rollup.schema_version == SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION
            && rollup.day_start_ms >= bootstrap_start_day_ms
            && rollup.day_start_ms <= bootstrap_end_day_ms
    }) {
        record_bootstrap_rollup(
            rollup,
            &mut stats,
            &mut daily_notional,
            &mut spread_samples,
            &mut gap_counts,
        );
    }

    finalize_bootstrap_stats(stats, daily_notional, spread_samples, gap_counts)
}

fn record_bootstrap_rollup(
    rollup: &SymbolUniverseBootstrapRollup,
    stats: &mut BTreeMap<String, SymbolStats>,
    daily_notional: &mut BTreeMap<String, BTreeMap<i64, f64>>,
    spread_samples: &mut BTreeMap<String, Vec<f64>>,
    gap_counts: &mut BTreeMap<String, (i64, i64)>,
) {
    for symbol in &rollup.symbols {
        ensure_bootstrap_symbol_stats(stats, symbol);
        daily_notional
            .entry(symbol.symbol_canonical.clone())
            .or_default()
            .entry(rollup.day_start_ms)
            .and_modify(|value| *value += symbol.traded_notional_sum)
            .or_insert(symbol.traded_notional_sum);
        spread_samples
            .entry(symbol.symbol_canonical.clone())
            .or_default()
            .extend(
                symbol
                    .spread_bps_median_samples
                    .iter()
                    .copied()
                    .filter(|value| value.is_finite()),
            );
        let gap_entry = gap_counts
            .entry(symbol.symbol_canonical.clone())
            .or_default();
        gap_entry.0 += symbol.gap_count;
        gap_entry.1 += symbol.window_count;
    }
}

fn ensure_bootstrap_symbol_stats(
    stats: &mut BTreeMap<String, SymbolStats>,
    symbol: &SymbolUniverseBootstrapSymbolStats,
) {
    let entry = stats
        .entry(symbol.symbol_canonical.clone())
        .or_insert_with(|| SymbolStats::from_bootstrap(symbol));
    if entry.execution_symbol_native.is_none() {
        entry.execution_symbol_native = symbol.execution_symbol_native.clone();
    }
    if entry.reference_symbol_native.is_none() {
        entry.reference_symbol_native = symbol.reference_symbol_native.clone();
    }
}

fn finalize_bootstrap_stats(
    mut stats: BTreeMap<String, SymbolStats>,
    daily_notional: BTreeMap<String, BTreeMap<i64, f64>>,
    spread_samples: BTreeMap<String, Vec<f64>>,
    gap_counts: BTreeMap<String, (i64, i64)>,
) -> BTreeMap<String, SymbolStats> {
    for (symbol, stat) in &mut stats {
        let day_values = daily_notional
            .get(symbol)
            .map(|by_day| by_day.values().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        stat.bootstrap_days_available = i64::try_from(day_values.len()).unwrap_or(i64::MAX);
        stat.median_traded_notional = median(day_values);
        stat.median_spread_bps = spread_samples.get(symbol).cloned().and_then(median);
        stat.gap_rate = gap_counts.get(symbol).map(|(gap_count, window_count)| {
            if *window_count <= 0 {
                1.0
            } else {
                *gap_count as f64 / *window_count as f64
            }
        });
    }
    stats
}
