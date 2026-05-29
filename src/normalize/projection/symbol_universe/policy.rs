use super::super::super::model::{SymbolLiquidityRank, SymbolUniverseMember};
use super::super::common::{
    MAX_APPROVED_RANK, MAX_GAP_RATE, MAX_MEDIAN_SPREAD_BPS, MIN_BOOTSTRAP_DAYS,
    MIN_REFERENCE_WARMUP_BOOTSTRAP_DAYS,
};
use super::stats::SymbolStats;
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub(super) fn liquidity_ranks(stats: &BTreeMap<String, SymbolStats>) -> Vec<SymbolLiquidityRank> {
    let mut rows = stats
        .values()
        .filter_map(|stat| {
            let notional = stat.median_traded_notional?;
            if !is_liquidity_rank_eligible(stat, notional) {
                return None;
            }
            Some((stat.symbol_canonical.clone(), notional))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    rows.into_iter()
        .enumerate()
        .map(
            |(index, (symbol_canonical, observed_traded_notional))| SymbolLiquidityRank {
                symbol_canonical,
                liquidity_rank_at_that_time: i64::try_from(index + 1).unwrap_or(i64::MAX),
                observed_traded_notional,
            },
        )
        .collect()
}

pub(super) fn is_approved_universe_symbol(stat: &SymbolStats, liquidity_rank: Option<i64>) -> bool {
    is_fully_bootstrapped_universe_symbol(stat, liquidity_rank)
}

fn is_fully_bootstrapped_universe_symbol(stat: &SymbolStats, liquidity_rank: Option<i64>) -> bool {
    stat.bootstrap_days_available >= MIN_BOOTSTRAP_DAYS
        && liquidity_rank.is_some_and(|rank| rank <= MAX_APPROVED_RANK)
        && passes_universe_quality(stat)
}

fn passes_universe_quality(stat: &SymbolStats) -> bool {
    stat.median_traded_notional
        .is_some_and(|notional| notional > 0.0)
        && stat
            .median_spread_bps
            .is_some_and(|spread| spread <= MAX_MEDIAN_SPREAD_BPS)
        && stat
            .gap_rate
            .is_some_and(|gap_rate| gap_rate <= MAX_GAP_RATE)
}

fn is_liquidity_rank_eligible(stat: &SymbolStats, notional: f64) -> bool {
    if !notional.is_finite() || notional <= 0.0 {
        return false;
    }
    if stat.bootstrap_days_available >= MIN_BOOTSTRAP_DAYS {
        return true;
    }
    stat.bootstrap_days_available >= MIN_REFERENCE_WARMUP_BOOTSTRAP_DAYS
        && stat.bootstrap_days_available < MIN_BOOTSTRAP_DAYS
        && is_reference_warmup_symbol(stat)
}

fn is_reference_warmup_symbol(stat: &SymbolStats) -> bool {
    stat.reference_symbol_native
        .as_deref()
        .is_some_and(|symbol| symbol.ends_with("USDT") && !symbol.starts_with("KRW-"))
}

pub(super) fn universe_status_reason(
    stat: &SymbolStats,
    liquidity_rank: Option<i64>,
    approved_universe_symbol: bool,
) -> String {
    if approved_universe_symbol {
        return "approved".to_owned();
    }
    if stat.bootstrap_days_available < MIN_BOOTSTRAP_DAYS {
        return "insufficient_30d_bootstrap".to_owned();
    }
    if liquidity_rank.is_none_or(|rank| rank > MAX_APPROVED_RANK) {
        return "outside_top_50_liquidity_rank".to_owned();
    }
    if !stat
        .median_traded_notional
        .is_some_and(|notional| notional > 0.0)
    {
        return "missing_30d_traded_notional".to_owned();
    }
    let Some(median_spread_bps) = stat.median_spread_bps else {
        return "missing_30d_spread".to_owned();
    };
    if median_spread_bps > MAX_MEDIAN_SPREAD_BPS {
        return "spread_too_wide_30d".to_owned();
    }
    if !stat
        .gap_rate
        .is_some_and(|gap_rate| gap_rate <= MAX_GAP_RATE)
    {
        return "gap_rate_too_high_30d".to_owned();
    }
    "not_admitted".to_owned()
}

pub(super) fn symbol_member_sort(
    left: &SymbolUniverseMember,
    right: &SymbolUniverseMember,
) -> Ordering {
    left.liquidity_rank_at_that_time
        .unwrap_or(i64::MAX)
        .cmp(&right.liquidity_rank_at_that_time.unwrap_or(i64::MAX))
        .then_with(|| left.symbol_canonical.cmp(&right.symbol_canonical))
}
