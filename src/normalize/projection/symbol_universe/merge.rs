use super::super::super::model::{
    SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION, SymbolUniverseBootstrapRollup,
};
use super::super::common::stable_id;
use super::stats::merge_symbol_rollup;
use std::collections::BTreeMap;

pub fn merge_symbol_universe_bootstrap_rollup(
    existing: Option<SymbolUniverseBootstrapRollup>,
    current: SymbolUniverseBootstrapRollup,
) -> SymbolUniverseBootstrapRollup {
    let Some(mut existing) = existing else {
        return current;
    };
    if existing.day_start_ms != current.day_start_ms
        || existing.schema_version != SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION
    {
        return current;
    }
    let current_window_seen = current.source_windows.iter().any(|window| {
        existing.source_windows.iter().any(|seen| {
            seen.source_window_start_ms == window.source_window_start_ms
                && seen.source_window_end_ms == window.source_window_end_ms
        })
    });
    if current_window_seen {
        return existing;
    }

    let mut by_symbol = existing
        .symbols
        .into_iter()
        .map(|symbol| (symbol.symbol_canonical.clone(), symbol))
        .collect::<BTreeMap<_, _>>();
    for symbol in current.symbols {
        by_symbol
            .entry(symbol.symbol_canonical.clone())
            .and_modify(|existing_symbol| merge_symbol_rollup(existing_symbol, &symbol))
            .or_insert(symbol);
    }
    existing.symbols = by_symbol.into_values().collect();
    existing
        .symbols
        .sort_by(|left, right| left.symbol_canonical.cmp(&right.symbol_canonical));
    existing.source_windows.extend(current.source_windows);
    existing.source_windows.sort();
    existing.source_windows.dedup_by(|left, right| {
        left.source_window_start_ms == right.source_window_start_ms
            && left.source_window_end_ms == right.source_window_end_ms
    });
    existing.generated_at_ms = current.generated_at_ms;
    existing.updated_by_l1_run_id = current.updated_by_l1_run_id;
    existing.rollup_id = stable_id(&[
        &existing.event_date,
        &existing.day_start_ms.to_string(),
        &existing.generated_at_ms.to_string(),
        SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION,
    ]);
    existing
}
