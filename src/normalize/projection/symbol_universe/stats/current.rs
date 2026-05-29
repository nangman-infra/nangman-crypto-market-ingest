use super::types::SymbolStats;
use crate::normalize::model::SliceRow;
use crate::normalize::projection::common::{ONE_DAY_MS, median, price};
use std::collections::BTreeMap;

pub(super) fn symbol_stats(slices: &[SliceRow]) -> BTreeMap<String, SymbolStats> {
    let mut daily = BTreeMap::<String, BTreeMap<i64, DailySymbolStats>>::new();
    let mut spreads = BTreeMap::<String, Vec<f64>>::new();
    let mut gap_counts = BTreeMap::<String, (i64, i64)>::new();
    let mut stats = BTreeMap::<String, SymbolStats>::new();
    for row in slices {
        let entry = stats
            .entry(row.symbol_canonical.clone())
            .or_insert_with(|| SymbolStats::from_slice(row));
        entry.observe_native_symbol(row);
        record_daily_symbol_stats(row, entry, &mut daily);
        record_symbol_spread(row, &mut spreads);
        record_symbol_gap(row, &mut gap_counts);
    }
    for (symbol, stat) in &mut stats {
        let daily_notional = daily
            .get(symbol)
            .map(|by_day| {
                by_day
                    .values()
                    .map(|day| day.traded_notional)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        stat.bootstrap_days_available = i64::try_from(daily_notional.len()).unwrap_or(i64::MAX);
        stat.median_traded_notional = median(daily_notional);
        stat.median_spread_bps = spreads.get(symbol).cloned().and_then(median);
        stat.gap_rate = gap_counts.get(symbol).map(|(gap_count, window_count)| {
            if *window_count == 0 {
                1.0
            } else {
                *gap_count as f64 / *window_count as f64
            }
        });
    }
    stats
}

fn record_daily_symbol_stats(
    row: &SliceRow,
    stat: &mut SymbolStats,
    daily: &mut BTreeMap<String, BTreeMap<i64, DailySymbolStats>>,
) {
    let traded_notional = row.trade_volume * price(row).unwrap_or(0.0);
    stat.observed_traded_notional += traded_notional;
    let day = row.window_start_ms.div_euclid(ONE_DAY_MS);
    daily
        .entry(row.symbol_canonical.clone())
        .or_default()
        .entry(day)
        .or_default()
        .traded_notional += traded_notional;
}

fn record_symbol_spread(row: &SliceRow, spreads: &mut BTreeMap<String, Vec<f64>>) {
    if let Some(spread_bps) = row.spread_bps.filter(|value| value.is_finite()) {
        spreads
            .entry(row.symbol_canonical.clone())
            .or_default()
            .push(spread_bps);
    }
}

fn record_symbol_gap(row: &SliceRow, gap_counts: &mut BTreeMap<String, (i64, i64)>) {
    let gap_entry = gap_counts.entry(row.symbol_canonical.clone()).or_default();
    gap_entry.0 += row.quality_gap;
    gap_entry.1 += 1;
}

#[derive(Debug, Clone, Default)]
struct DailySymbolStats {
    traded_notional: f64,
}
