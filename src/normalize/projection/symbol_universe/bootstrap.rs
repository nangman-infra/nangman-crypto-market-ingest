use super::super::super::args::InputRange;
use super::super::super::model::{
    SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION, SliceRow, SymbolUniverseBootstrapRollup,
    SymbolUniverseBootstrapSourceWindow, SymbolUniverseBootstrapSymbolStats,
};
use super::super::common::{day_start_ms, event_date, stable_id};
use super::stats::BootstrapRunSymbolAccumulator;
use std::collections::BTreeMap;

pub fn build_symbol_universe_bootstrap_rollups(
    l1_run_id: &str,
    input_range: InputRange,
    generated_at_ms: i64,
    slices: &[SliceRow],
) -> Vec<SymbolUniverseBootstrapRollup> {
    let mut by_day_symbol = BTreeMap::<(i64, String), BootstrapRunSymbolAccumulator>::new();
    for row in slices {
        let day_start_ms = day_start_ms(row.window_start_ms);
        let entry = by_day_symbol
            .entry((day_start_ms, row.symbol_canonical.clone()))
            .or_insert_with(|| BootstrapRunSymbolAccumulator {
                symbol_canonical: row.symbol_canonical.clone(),
                execution_symbol_native: None,
                reference_symbol_native: None,
                traded_notional_sum: 0.0,
                spread_samples: Vec::new(),
                gap_count: 0,
                window_count: 0,
                mapping_confidence: "moderate".to_owned(),
            });
        entry.observe_slice(row);
    }

    let mut by_day = BTreeMap::<i64, Vec<SymbolUniverseBootstrapSymbolStats>>::new();
    for ((day_start_ms, _), accumulator) in by_day_symbol {
        by_day
            .entry(day_start_ms)
            .or_default()
            .push(accumulator.into_symbol_stats());
    }

    by_day
        .into_iter()
        .map(|(day_start_ms, mut symbols)| {
            symbols.sort_by(|left, right| left.symbol_canonical.cmp(&right.symbol_canonical));
            let event_date = event_date(day_start_ms);
            SymbolUniverseBootstrapRollup {
                schema_version: SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION.to_owned(),
                rollup_id: stable_id(&[
                    l1_run_id,
                    &input_range.start_ms.to_string(),
                    &input_range.end_ms.to_string(),
                    &day_start_ms.to_string(),
                    SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION,
                ]),
                event_date,
                day_start_ms,
                generated_at_ms,
                updated_by_l1_run_id: l1_run_id.to_owned(),
                source_windows: vec![SymbolUniverseBootstrapSourceWindow {
                    l1_run_id: l1_run_id.to_owned(),
                    source_window_start_ms: input_range.start_ms,
                    source_window_end_ms: input_range.end_ms,
                }],
                symbols,
            }
        })
        .collect()
}
