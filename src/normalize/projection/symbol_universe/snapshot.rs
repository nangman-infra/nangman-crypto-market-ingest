use super::super::super::args::InputRange;
use super::super::super::model::{
    SYMBOL_UNIVERSE_SNAPSHOT_SCHEMA_VERSION, SliceRow, SymbolUniverseBootstrapRollup,
    SymbolUniverseMember, SymbolUniverseSnapshot,
};
use super::super::common::{
    DATA_QUALITY_CUTOFF_VERSION, SELECTION_POLICY_VERSION, VENUE_TRUTH_POLICY_VERSION, stable_id,
};
use super::bootstrap::build_symbol_universe_bootstrap_rollups;
use super::policy::{
    is_approved_universe_symbol, liquidity_ranks, symbol_member_sort, universe_status_reason,
};
use super::stats::symbol_stats_from_bootstrap;
use std::collections::BTreeMap;

pub fn build_symbol_universe_snapshot(
    l1_run_id: &str,
    input_range: InputRange,
    generated_at_ms: i64,
    slices: &[SliceRow],
) -> SymbolUniverseSnapshot {
    let rollups =
        build_symbol_universe_bootstrap_rollups(l1_run_id, input_range, generated_at_ms, slices);
    build_symbol_universe_snapshot_from_bootstrap(
        l1_run_id,
        input_range,
        generated_at_ms,
        slices,
        &rollups,
    )
}

pub fn build_symbol_universe_snapshot_from_bootstrap(
    l1_run_id: &str,
    input_range: InputRange,
    generated_at_ms: i64,
    current_slices: &[SliceRow],
    bootstrap_rollups: &[SymbolUniverseBootstrapRollup],
) -> SymbolUniverseSnapshot {
    let stats = symbol_stats_from_bootstrap(input_range, current_slices, bootstrap_rollups);
    let liquidity_rank_at_that_time = liquidity_ranks(&stats);
    let rank_by_symbol = liquidity_rank_at_that_time
        .iter()
        .map(|rank| {
            (
                rank.symbol_canonical.clone(),
                rank.liquidity_rank_at_that_time,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut included_symbols = Vec::new();
    let mut excluded_symbols = Vec::new();
    for stat in stats.values() {
        let liquidity_rank = rank_by_symbol.get(&stat.symbol_canonical).copied();
        let approved_universe_symbol = is_approved_universe_symbol(stat, liquidity_rank);
        let member = SymbolUniverseMember {
            symbol_canonical: stat.symbol_canonical.clone(),
            execution_symbol_native: stat.execution_symbol_native.clone(),
            reference_symbol_native: stat.reference_symbol_native.clone(),
            liquidity_rank_at_that_time: liquidity_rank,
            approved_universe_symbol,
            bootstrap_days_available: stat.bootstrap_days_available,
            median_spread_bps_30d: stat.median_spread_bps,
            median_traded_notional_30d: stat.median_traded_notional,
            gap_rate_30d: stat.gap_rate,
            mapping_confidence: stat.mapping_confidence.clone(),
            status_reason: universe_status_reason(stat, liquidity_rank, approved_universe_symbol),
        };
        if approved_universe_symbol {
            included_symbols.push(member);
        } else {
            excluded_symbols.push(member);
        }
    }
    included_symbols.sort_by(symbol_member_sort);
    excluded_symbols.sort_by(symbol_member_sort);

    SymbolUniverseSnapshot {
        schema_version: SYMBOL_UNIVERSE_SNAPSHOT_SCHEMA_VERSION.to_owned(),
        symbol_universe_snapshot_id: stable_id(&[
            l1_run_id,
            &input_range.start_ms.to_string(),
            &input_range.end_ms.to_string(),
            SYMBOL_UNIVERSE_SNAPSHOT_SCHEMA_VERSION,
        ]),
        universe_as_of_ms: input_range.end_ms,
        included_symbols,
        excluded_symbols,
        liquidity_rank_at_that_time,
        selection_policy_version: SELECTION_POLICY_VERSION.to_owned(),
        venue_truth_policy_version: VENUE_TRUTH_POLICY_VERSION.to_owned(),
        data_quality_cutoff_version: DATA_QUALITY_CUTOFF_VERSION.to_owned(),
        generated_at_ms,
    }
}
