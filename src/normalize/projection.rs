use super::args::InputRange;
use super::derivative_projection::build_derivative_feature_deltas;
use super::model::{
    DerivativeMetricObservation, MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION,
    MARKET_FEATURE_DELTA_SCHEMA_VERSION, MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION,
    MARKET_REGIME_CONTEXT_SCHEMA_VERSION, MarketDataQualitySummary, MarketFeatureDelta,
    MarketFeatureDeltaSummary, MarketFeatureDeltaSummaryMetric, MarketFeatureDeltaSummaryRow,
    MarketRegimeContext, SYMBOL_UNIVERSE_BOOTSTRAP_ROLLUP_SCHEMA_VERSION,
    SYMBOL_UNIVERSE_SNAPSHOT_SCHEMA_VERSION, SliceRow, SymbolLiquidityRank,
    SymbolUniverseBootstrapRollup, SymbolUniverseBootstrapSourceWindow,
    SymbolUniverseBootstrapSymbolStats, SymbolUniverseMember, SymbolUniverseSnapshot,
};
use crate::storage::record::sha256_hex;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const SELECTION_POLICY_VERSION: &str = "observed_liquidity_rank_p0_v1";
const VENUE_TRUTH_POLICY_VERSION: &str = "execution_reference_split_p0_v1";
const DATA_QUALITY_CUTOFF_VERSION: &str = "requires_30d_or_reference_warmup_p0_v1";
const FIFTEEN_MINUTES_MS: i64 = 900_000;
const ONE_HOUR_MS: i64 = 3_600_000;
const ONE_DAY_MS: i64 = 86_400_000;
const MIN_BOOTSTRAP_DAYS: i64 = 30;
const BOOTSTRAP_ROLLUP_DAYS: i64 = 30;
const MAX_APPROVED_RANK: i64 = 50;
const MIN_REFERENCE_WARMUP_BOOTSTRAP_DAYS: i64 = 1;
const MAX_REFERENCE_WARMUP_RANK: i64 = 50;
const MAX_MEDIAN_SPREAD_BPS: f64 = 50.0;
const MAX_GAP_RATE: f64 = 0.05;

pub fn build_market_data_quality_summary(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    slices: &[SliceRow],
) -> MarketDataQualitySummary {
    let coverage_ratio = if slices.is_empty() {
        0.0
    } else {
        let covered = slices
            .iter()
            .filter(|row| {
                matches!(
                    row.slice_completeness.as_str(),
                    "complete" | "partial" | "reference_only"
                )
            })
            .count();
        covered as f64 / slices.len() as f64
    };
    let gap_count = slices.iter().map(|row| row.quality_gap).sum();
    let stale_sources = sources_by_quality(slices, |row| row.quality_stale > 0);
    let delayed_sources = sources_by_quality(slices, |row| row.quality_delayed > 0);

    MarketDataQualitySummary {
        schema_version: MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION.to_owned(),
        quality_summary_id: stable_id(&[
            l1_run_id,
            &input_range.start_ms.to_string(),
            &input_range.end_ms.to_string(),
            MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        coverage_ratio,
        gap_count,
        stale_sources,
        delayed_sources,
        missing_venues: missing_venues(slices),
        source_health_status: source_health_status(slices),
        symbol_health_status: symbol_health_status(slices),
        quality_window_start_ms: input_range.start_ms,
        quality_window_end_ms: input_range.end_ms,
        known_as_of_ms,
    }
}

pub fn build_market_feature_deltas(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    projection_slices: &[SliceRow],
    projection_derivative_metrics: &[DerivativeMetricObservation],
) -> Vec<MarketFeatureDelta> {
    let grouped = group_slices_by_symbol(projection_slices);
    let mut deltas = Vec::new();
    for rows in grouped.values() {
        for row in rows
            .iter()
            .copied()
            .filter(|row| row.window_start_ms >= input_range.start_ms)
        {
            let price_now = price(row);
            let volume_now = Some(row.trade_volume);
            let price_15m =
                value_at_or_before(rows, row.window_start_ms - FIFTEEN_MINUTES_MS, price);
            let price_1h = value_at_or_before(rows, row.window_start_ms - ONE_HOUR_MS, price);
            let volume_15m =
                value_at_or_before(rows, row.window_start_ms - FIFTEEN_MINUTES_MS, volume);
            let volume_1h = value_at_or_before(rows, row.window_start_ms - ONE_HOUR_MS, volume);
            let price_change_15m = percent_change(price_now, price_15m);
            let price_change_1h = percent_change(price_now, price_1h);
            let volume_change_15m = percent_change(volume_now, volume_15m);
            let volume_change_1h = percent_change(volume_now, volume_1h);

            if let Some(value_now) = price_now {
                deltas.push(market_feature_delta(
                    l1_run_id,
                    "price",
                    row,
                    MarketFeatureDeltaValues {
                        value_now,
                        value_15m_ago: price_15m,
                        value_1h_ago: price_1h,
                        change_pct_15m: price_change_15m,
                        change_pct_1h: price_change_1h,
                        price_change_same_window: price_change_1h,
                        volume_change_same_window: volume_change_1h,
                        oi_price_divergence: None,
                        known_as_of_ms,
                    },
                ));
            }

            deltas.push(market_feature_delta(
                l1_run_id,
                "trade_volume",
                row,
                MarketFeatureDeltaValues {
                    value_now: row.trade_volume,
                    value_15m_ago: volume_15m,
                    value_1h_ago: volume_1h,
                    change_pct_15m: volume_change_15m,
                    change_pct_1h: volume_change_1h,
                    price_change_same_window: price_change_1h,
                    volume_change_same_window: volume_change_1h,
                    oi_price_divergence: None,
                    known_as_of_ms,
                },
            ));
        }
    }
    deltas.extend(build_derivative_feature_deltas(
        l1_run_id,
        input_range,
        projection_derivative_metrics,
    ));
    deltas.sort_by(|left, right| {
        left.window_start_ms
            .cmp(&right.window_start_ms)
            .then_with(|| left.venue.cmp(&right.venue))
            .then_with(|| left.symbol_canonical.cmp(&right.symbol_canonical))
            .then_with(|| left.metric_name.cmp(&right.metric_name))
    });
    deltas
}

pub fn build_market_feature_delta_summary(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    detail_feature_delta_key: &str,
    deltas: &[MarketFeatureDelta],
) -> MarketFeatureDeltaSummary {
    let mut grouped =
        BTreeMap::<MarketFeatureDeltaSummaryKey, BTreeMap<String, &MarketFeatureDelta>>::new();
    for delta in deltas {
        let metrics = grouped
            .entry(MarketFeatureDeltaSummaryKey {
                venue: delta.venue.clone(),
                symbol_native: delta.symbol_native.clone(),
                symbol_canonical: delta.symbol_canonical.clone(),
                market_type: delta.market_type.clone(),
            })
            .or_default();
        match metrics.get(&delta.metric_name) {
            Some(existing) if !is_newer_delta(delta, existing) => {}
            _ => {
                metrics.insert(delta.metric_name.clone(), delta);
            }
        }
    }

    let rows = grouped
        .into_iter()
        .map(|(key, metrics)| {
            let mut accumulator =
                MarketFeatureDeltaSummaryAccumulator::new(input_range, known_as_of_ms);
            let metrics = metrics
                .into_values()
                .map(|delta| accumulator.observe(delta))
                .collect::<Vec<_>>();
            accumulator.into_row(key, metrics)
        })
        .collect::<Vec<_>>();

    MarketFeatureDeltaSummary {
        schema_version: MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION.to_owned(),
        feature_delta_summary_id: stable_id(&[
            l1_run_id,
            &input_range.start_ms.to_string(),
            &input_range.end_ms.to_string(),
            detail_feature_delta_key,
            MARKET_FEATURE_DELTA_SUMMARY_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        detail_feature_delta_key: detail_feature_delta_key.to_owned(),
        window_start_ms: input_range.start_ms,
        window_end_ms: input_range.end_ms,
        known_as_of_ms,
        detail_record_count: deltas.len(),
        summary_row_count: rows.len(),
        rows,
    }
}

struct MarketFeatureDeltaSummaryAccumulator {
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
    fn new(input_range: InputRange, fallback_known_as_of_ms: i64) -> Self {
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

    fn observe(&mut self, delta: &MarketFeatureDelta) -> MarketFeatureDeltaSummaryMetric {
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

    fn into_row(
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

pub fn build_market_regime_contexts(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    projection_slices: &[SliceRow],
) -> Vec<MarketRegimeContext> {
    let returns_by_window = return_samples_by_window(projection_slices);
    let mut contexts = Vec::new();
    for (window_start_ms, samples) in returns_by_window
        .iter()
        .filter(|(window_start_ms, _)| **window_start_ms >= input_range.start_ms)
    {
        contexts.push(regime_context_for_window(
            l1_run_id,
            known_as_of_ms,
            &returns_by_window,
            *window_start_ms,
            samples,
        ));
    }
    contexts
}

fn regime_context_for_window(
    l1_run_id: &str,
    known_as_of_ms: i64,
    returns_by_window: &BTreeMap<i64, Vec<ReturnSample>>,
    window_start_ms: i64,
    samples: &[ReturnSample],
) -> MarketRegimeContext {
    let btc_return = return_for_symbol(samples, "BTC");
    let eth_return = return_for_symbol(samples, "ETH");
    let sector_return = mean(samples.iter().map(|sample| sample.return_pct));
    let volatility = population_stddev(samples.iter().map(|sample| sample.return_pct));
    let correlation_to_btc = rolling_correlation_to_btc(returns_by_window, window_start_ms);
    let mut missing_reasons =
        regime_missing_reasons(btc_return, eth_return, sector_return, correlation_to_btc);
    if samples
        .iter()
        .any(|sample| sample.lookback_ms < ONE_HOUR_MS)
    {
        missing_reasons.push("return_lookback_degraded".to_owned());
    }
    MarketRegimeContext {
        schema_version: MARKET_REGIME_CONTEXT_SCHEMA_VERSION.to_owned(),
        regime_context_id: stable_id(&[
            l1_run_id,
            &window_start_ms.to_string(),
            "market_all_symbols",
            MARKET_REGIME_CONTEXT_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        scope: "market_all_symbols".to_owned(),
        window_start_ms,
        window_end_ms: samples
            .first()
            .map(|sample| sample.window_end_ms)
            .unwrap_or_else(|| window_start_ms.saturating_add(1_000)),
        btc_return_same_window: btc_return,
        eth_return_same_window: eth_return,
        sector_return_same_window: sector_return,
        volatility_regime: volatility_regime(volatility),
        correlation_to_btc,
        known_as_of_ms,
        quality_status: regime_quality_status(&missing_reasons, sector_return).to_owned(),
        missing_reasons,
    }
}

fn return_for_symbol(samples: &[ReturnSample], symbol: &str) -> Option<f64> {
    samples
        .iter()
        .find(|sample| sample.symbol_canonical == symbol)
        .map(|sample| sample.return_pct)
}

fn regime_missing_reasons(
    btc_return: Option<f64>,
    eth_return: Option<f64>,
    sector_return: Option<f64>,
    correlation_to_btc: Option<f64>,
) -> Vec<String> {
    let mut missing_reasons = Vec::new();
    push_missing_if_none(&mut missing_reasons, btc_return, "btc_return_missing");
    push_missing_if_none(&mut missing_reasons, eth_return, "eth_return_missing");
    push_missing_if_none(&mut missing_reasons, sector_return, "sector_return_missing");
    push_missing_if_none(
        &mut missing_reasons,
        correlation_to_btc,
        "correlation_to_btc_insufficient_samples",
    );
    missing_reasons
}

fn push_missing_if_none<T>(missing_reasons: &mut Vec<String>, value: Option<T>, reason: &str) {
    if value.is_none() {
        missing_reasons.push(reason.to_owned());
    }
}

fn regime_quality_status(missing_reasons: &[String], sector_return: Option<f64>) -> &'static str {
    if missing_reasons.is_empty() {
        "complete"
    } else if sector_return.is_some() {
        "partial"
    } else {
        "insufficient"
    }
}

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

fn sources_by_quality(slices: &[SliceRow], is_flagged: impl Fn(&SliceRow) -> bool) -> Vec<String> {
    slices
        .iter()
        .filter(|row| is_flagged(row))
        .map(|row| row.venue.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn missing_venues(slices: &[SliceRow]) -> Vec<String> {
    let mut venues = BTreeSet::new();
    for row in slices {
        if row.slice_completeness == "incomplete"
            || row.missing_reasons.iter().any(|reason| {
                reason.contains("missing") || reason.contains("gap") || reason.contains("stale")
            })
        {
            venues.insert(row.venue.clone());
        }
    }
    venues.into_iter().collect()
}

fn source_health_status(slices: &[SliceRow]) -> String {
    let mut has_unknown = false;
    for row in slices {
        let Some(snapshot) = row.source_health_snapshot.as_ref() else {
            has_unknown = true;
            continue;
        };
        if snapshot.health_level != "healthy" && snapshot.health_level != "ok" {
            return "degraded".to_owned();
        }
    }
    if has_unknown {
        "unknown".to_owned()
    } else {
        "healthy".to_owned()
    }
}

fn symbol_health_status(slices: &[SliceRow]) -> String {
    let mut has_unknown = false;
    for row in slices {
        let Some(snapshot) = row.symbol_health_snapshot.as_ref() else {
            has_unknown = true;
            continue;
        };
        if !snapshot.is_tradeable || row.quality_gap > 0 || row.quality_stale > 0 {
            return "degraded".to_owned();
        }
    }
    if has_unknown {
        "unknown".to_owned()
    } else {
        "healthy".to_owned()
    }
}

#[derive(Debug, Clone)]
struct SymbolStats {
    symbol_canonical: String,
    execution_symbol_native: Option<String>,
    reference_symbol_native: Option<String>,
    observed_traded_notional: f64,
    bootstrap_days_available: i64,
    median_spread_bps: Option<f64>,
    median_traded_notional: Option<f64>,
    gap_rate: Option<f64>,
    mapping_confidence: String,
}

impl SymbolStats {
    fn from_slice(row: &SliceRow) -> Self {
        Self {
            symbol_canonical: row.symbol_canonical.clone(),
            execution_symbol_native: None,
            reference_symbol_native: None,
            observed_traded_notional: 0.0,
            bootstrap_days_available: 0,
            median_spread_bps: None,
            median_traded_notional: None,
            gap_rate: None,
            mapping_confidence: "moderate".to_owned(),
        }
    }

    fn from_bootstrap(symbol: &SymbolUniverseBootstrapSymbolStats) -> Self {
        Self {
            symbol_canonical: symbol.symbol_canonical.clone(),
            execution_symbol_native: symbol.execution_symbol_native.clone(),
            reference_symbol_native: symbol.reference_symbol_native.clone(),
            observed_traded_notional: 0.0,
            bootstrap_days_available: 0,
            median_spread_bps: None,
            median_traded_notional: None,
            gap_rate: None,
            mapping_confidence: symbol.mapping_confidence.clone(),
        }
    }

    fn observe_native_symbol(&mut self, row: &SliceRow) {
        assign_native_symbols(
            row,
            &mut self.execution_symbol_native,
            &mut self.reference_symbol_native,
        );
    }
}

#[derive(Debug, Clone)]
struct BootstrapRunSymbolAccumulator {
    symbol_canonical: String,
    execution_symbol_native: Option<String>,
    reference_symbol_native: Option<String>,
    traded_notional_sum: f64,
    spread_samples: Vec<f64>,
    gap_count: i64,
    window_count: i64,
    mapping_confidence: String,
}

impl BootstrapRunSymbolAccumulator {
    fn observe_slice(&mut self, row: &SliceRow) {
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

    fn into_symbol_stats(self) -> SymbolUniverseBootstrapSymbolStats {
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

fn symbol_stats(slices: &[SliceRow]) -> BTreeMap<String, SymbolStats> {
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

fn assign_native_symbols(
    row: &SliceRow,
    execution_symbol_native: &mut Option<String>,
    reference_symbol_native: &mut Option<String>,
) {
    if row.venue == "upbit" && execution_symbol_native.is_none() {
        *execution_symbol_native = Some(row.symbol_native.clone());
    }
    if row.venue == "binance" && reference_symbol_native.is_none() {
        *reference_symbol_native = Some(row.symbol_native.clone());
    }
    execution_symbol_native.get_or_insert_with(|| row.symbol_native.clone());
    reference_symbol_native.get_or_insert_with(|| row.symbol_native.clone());
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

fn symbol_stats_from_bootstrap(
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
        for symbol in &rollup.symbols {
            let entry = stats
                .entry(symbol.symbol_canonical.clone())
                .or_insert_with(|| SymbolStats::from_bootstrap(symbol));
            if entry.execution_symbol_native.is_none() {
                entry.execution_symbol_native = symbol.execution_symbol_native.clone();
            }
            if entry.reference_symbol_native.is_none() {
                entry.reference_symbol_native = symbol.reference_symbol_native.clone();
            }
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

fn merge_symbol_rollup(
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

#[derive(Debug, Clone, Default)]
struct DailySymbolStats {
    traded_notional: f64,
}

fn liquidity_ranks(stats: &BTreeMap<String, SymbolStats>) -> Vec<SymbolLiquidityRank> {
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

fn stable_id(parts: &[&str]) -> String {
    sha256_hex(parts.join("|").as_bytes())
}

pub fn bootstrap_rollup_day_starts(input_range: InputRange) -> Vec<i64> {
    let end_day_start_ms = day_start_ms(input_range.end_ms.saturating_sub(1));
    (0..BOOTSTRAP_ROLLUP_DAYS)
        .rev()
        .map(|offset| end_day_start_ms.saturating_sub(offset * ONE_DAY_MS))
        .collect()
}

fn day_start_ms(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(ONE_DAY_MS) * ONE_DAY_MS
}

fn event_date(day_start_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(day_start_ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .format("%Y-%m-%d")
        .to_string()
}

fn group_slices_by_symbol(slices: &[SliceRow]) -> BTreeMap<SymbolWindowKey, Vec<&SliceRow>> {
    let mut grouped = BTreeMap::<SymbolWindowKey, Vec<&SliceRow>>::new();
    for row in slices {
        grouped
            .entry(SymbolWindowKey {
                venue: row.venue.clone(),
                symbol_native: row.symbol_native.clone(),
                symbol_canonical: row.symbol_canonical.clone(),
                market_type: row.market_type.clone(),
            })
            .or_default()
            .push(row);
    }
    for rows in grouped.values_mut() {
        rows.sort_by_key(|row| row.window_start_ms);
    }
    grouped
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SymbolWindowKey {
    venue: String,
    symbol_native: String,
    symbol_canonical: String,
    market_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MarketFeatureDeltaSummaryKey {
    venue: String,
    symbol_native: String,
    symbol_canonical: String,
    market_type: String,
}

fn is_newer_delta(candidate: &MarketFeatureDelta, existing: &MarketFeatureDelta) -> bool {
    candidate
        .window_end_ms
        .cmp(&existing.window_end_ms)
        .then_with(|| candidate.window_start_ms.cmp(&existing.window_start_ms))
        .then_with(|| candidate.known_as_of_ms.cmp(&existing.known_as_of_ms))
        == Ordering::Greater
}

fn market_feature_delta(
    l1_run_id: &str,
    metric_name: &str,
    row: &SliceRow,
    values: MarketFeatureDeltaValues,
) -> MarketFeatureDelta {
    let mut missing_reasons = Vec::new();
    if values.value_15m_ago.is_none() {
        missing_reasons.push("value_15m_ago_missing".to_owned());
    }
    if values.value_1h_ago.is_none() {
        missing_reasons.push("value_1h_ago_missing".to_owned());
    }
    if values.price_change_same_window.is_none() {
        missing_reasons.push("price_change_same_window_missing".to_owned());
    }
    if values.volume_change_same_window.is_none() {
        missing_reasons.push("volume_change_same_window_missing".to_owned());
    }
    let quality_status = if missing_reasons.is_empty() {
        "complete"
    } else if values.change_pct_15m.is_some() || values.change_pct_1h.is_some() {
        "partial"
    } else {
        "insufficient"
    };
    MarketFeatureDelta {
        schema_version: MARKET_FEATURE_DELTA_SCHEMA_VERSION.to_owned(),
        feature_delta_id: stable_id(&[
            l1_run_id,
            metric_name,
            row.venue.as_str(),
            row.symbol_native.as_str(),
            &row.window_start_ms.to_string(),
            MARKET_FEATURE_DELTA_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        metric_name: metric_name.to_owned(),
        venue: row.venue.clone(),
        symbol_native: row.symbol_native.clone(),
        symbol_canonical: row.symbol_canonical.clone(),
        market_type: row.market_type.clone(),
        value_now: values.value_now,
        value_15m_ago: values.value_15m_ago,
        value_1h_ago: values.value_1h_ago,
        change_pct_15m: values.change_pct_15m,
        change_pct_1h: values.change_pct_1h,
        price_change_same_window: values.price_change_same_window,
        volume_change_same_window: values.volume_change_same_window,
        oi_price_divergence: values.oi_price_divergence,
        window_start_ms: row.window_start_ms,
        window_end_ms: row.window_end_ms,
        known_as_of_ms: values.known_as_of_ms,
        quality_status: quality_status.to_owned(),
        missing_reasons,
    }
}

#[derive(Debug, Clone, Copy)]
struct MarketFeatureDeltaValues {
    value_now: f64,
    value_15m_ago: Option<f64>,
    value_1h_ago: Option<f64>,
    change_pct_15m: Option<f64>,
    change_pct_1h: Option<f64>,
    price_change_same_window: Option<f64>,
    volume_change_same_window: Option<f64>,
    oi_price_divergence: Option<f64>,
    known_as_of_ms: i64,
}

fn price(row: &SliceRow) -> Option<f64> {
    row.mid_price
        .or(row.last_trade_price)
        .filter(|value| value.is_finite() && *value > 0.0)
}

fn volume(row: &SliceRow) -> Option<f64> {
    Some(row.trade_volume).filter(|value| value.is_finite() && *value >= 0.0)
}

fn value_at_or_before(
    rows: &[&SliceRow],
    target_window_start_ms: i64,
    value: impl Fn(&SliceRow) -> Option<f64>,
) -> Option<f64> {
    // rows are sorted by window_start_ms ascending (see group_slices_by_symbol).
    let idx = rows.partition_point(|row| row.window_start_ms <= target_window_start_ms);
    if idx == 0 {
        return None;
    }
    value(rows[idx - 1])
}

fn percent_change(now: Option<f64>, previous: Option<f64>) -> Option<f64> {
    let now = now?;
    let previous = previous?;
    if !now.is_finite() || !previous.is_finite() || previous.abs() <= f64::EPSILON {
        return None;
    }
    Some(((now - previous) / previous) * 100.0)
}

#[derive(Debug, Clone)]
struct ReturnSample {
    symbol_canonical: String,
    window_end_ms: i64,
    return_pct: f64,
    lookback_ms: i64,
}

fn return_samples_by_window(slices: &[SliceRow]) -> BTreeMap<i64, Vec<ReturnSample>> {
    let grouped = group_slices_by_symbol(slices);
    let mut by_window = BTreeMap::<i64, Vec<ReturnSample>>::new();
    for rows in grouped.values() {
        for row in rows.iter().copied() {
            if let Some((return_pct, lookback_ms)) = return_sample_for_row(rows, row) {
                by_window
                    .entry(row.window_start_ms)
                    .or_default()
                    .push(ReturnSample {
                        symbol_canonical: row.symbol_canonical.clone(),
                        window_end_ms: row.window_end_ms,
                        return_pct,
                        lookback_ms,
                    });
            }
        }
    }
    by_window
}

fn return_sample_for_row(rows: &[&SliceRow], row: &SliceRow) -> Option<(f64, i64)> {
    let current_price = price(row)?;
    let target_start_ms = row.window_start_ms.saturating_sub(ONE_HOUR_MS);
    let historical_price = price_with_window_at_or_before(rows, target_start_ms)
        .or_else(|| nearest_prior_price_with_window(rows, row.window_start_ms))?;
    let return_pct = percent_change(Some(current_price), Some(historical_price.0))?;
    Some((
        return_pct,
        row.window_start_ms.saturating_sub(historical_price.1),
    ))
}

fn price_with_window_at_or_before(rows: &[&SliceRow], target_ms: i64) -> Option<(f64, i64)> {
    rows.iter()
        .rev()
        .find(|row| row.window_start_ms <= target_ms)
        .and_then(|row| price(row).map(|value| (value, row.window_start_ms)))
}

fn nearest_prior_price_with_window(
    rows: &[&SliceRow],
    current_window_start_ms: i64,
) -> Option<(f64, i64)> {
    rows.iter()
        .rev()
        .find(|row| row.window_start_ms < current_window_start_ms)
        .and_then(|row| price(row).map(|value| (value, row.window_start_ms)))
}

fn rolling_correlation_to_btc(
    returns_by_window: &BTreeMap<i64, Vec<ReturnSample>>,
    current_window_start_ms: i64,
) -> Option<f64> {
    let start_ms = current_window_start_ms.saturating_sub(ONE_HOUR_MS);
    let mut btc_returns = Vec::new();
    let mut sector_returns = Vec::new();
    for (_, samples) in returns_by_window.range(start_ms..=current_window_start_ms) {
        let btc = samples
            .iter()
            .find(|sample| sample.symbol_canonical == "BTC")
            .map(|sample| sample.return_pct)?;
        let sector = mean(samples.iter().map(|sample| sample.return_pct))?;
        btc_returns.push(btc);
        sector_returns.push(sector);
    }
    correlation(&btc_returns, &sector_returns)
}

fn mean(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut count = 0usize;
    let mut sum = 0.0;
    for value in values.filter(|value| value.is_finite()) {
        count += 1;
        sum += value;
    }
    (count > 0).then(|| sum / count as f64)
}

/// Population standard deviation (divisor = N).
///
/// volatility_regime treats the observed window as the entity to describe,
/// not as a sample drawn from a larger population. If a caller ever needs a
/// sample estimator, add `sample_stddev` (divisor = N - 1) explicitly so the
/// distinction stays in the call site, not the helper name.
fn population_stddev(values: impl Iterator<Item = f64>) -> Option<f64> {
    let values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    Some(variance.sqrt())
}

fn correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 3 {
        return None;
    }
    let left_mean = left.iter().sum::<f64>() / left.len() as f64;
    let right_mean = right.iter().sum::<f64>() / right.len() as f64;
    let mut numerator = 0.0;
    let mut left_denominator = 0.0;
    let mut right_denominator = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_delta = left_value - left_mean;
        let right_delta = right_value - right_mean;
        numerator += left_delta * right_delta;
        left_denominator += left_delta.powi(2);
        right_denominator += right_delta.powi(2);
    }
    if left_denominator <= f64::EPSILON || right_denominator <= f64::EPSILON {
        return None;
    }
    Some(numerator / (left_denominator.sqrt() * right_denominator.sqrt()))
}

fn volatility_regime(volatility: Option<f64>) -> String {
    match volatility {
        Some(value) if value < 0.5 => "low".to_owned(),
        Some(value) if value < 2.0 => "medium".to_owned(),
        Some(_) => "high".to_owned(),
        None => "unknown".to_owned(),
    }
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[middle - 1] + values[middle]) / 2.0)
    } else {
        Some(values[middle])
    }
}

fn is_approved_universe_symbol(stat: &SymbolStats, liquidity_rank: Option<i64>) -> bool {
    is_fully_bootstrapped_universe_symbol(stat, liquidity_rank)
        || is_reference_warmup_universe_symbol(stat, liquidity_rank)
}

fn is_fully_bootstrapped_universe_symbol(stat: &SymbolStats, liquidity_rank: Option<i64>) -> bool {
    stat.bootstrap_days_available >= MIN_BOOTSTRAP_DAYS
        && liquidity_rank.is_some_and(|rank| rank <= MAX_APPROVED_RANK)
        && passes_universe_quality(stat)
}

fn is_reference_warmup_universe_symbol(stat: &SymbolStats, liquidity_rank: Option<i64>) -> bool {
    stat.bootstrap_days_available >= MIN_REFERENCE_WARMUP_BOOTSTRAP_DAYS
        && stat.bootstrap_days_available < MIN_BOOTSTRAP_DAYS
        && is_reference_warmup_symbol(stat)
        && liquidity_rank.is_some_and(|rank| rank <= MAX_REFERENCE_WARMUP_RANK)
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

fn universe_status_reason(
    stat: &SymbolStats,
    liquidity_rank: Option<i64>,
    approved_universe_symbol: bool,
) -> String {
    if approved_universe_symbol {
        if is_reference_warmup_universe_symbol(stat, liquidity_rank) {
            return "approved_reference_warmup".to_owned();
        }
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
    if !stat
        .median_spread_bps
        .is_some_and(|spread| spread <= MAX_MEDIAN_SPREAD_BPS)
    {
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

fn symbol_member_sort(left: &SymbolUniverseMember, right: &SymbolUniverseMember) -> Ordering {
    left.liquidity_rank_at_that_time
        .unwrap_or(i64::MAX)
        .cmp(&right.liquidity_rank_at_that_time.unwrap_or(i64::MAX))
        .then_with(|| left.symbol_canonical.cmp(&right.symbol_canonical))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_summary_preserves_known_as_of_boundary() {
        let slices = vec![slice("upbit", "BTC", "KRW-BTC", 100.0, 2.0, "complete")];

        let summary = build_market_data_quality_summary(
            "run-1",
            InputRange {
                start_ms: 1_000,
                end_ms: 2_000,
            },
            2_500,
            &slices,
        );

        assert_eq!(summary.schema_version, "market_data_quality_summary_v1");
        assert_eq!(summary.coverage_ratio, 1.0);
        assert_eq!(summary.known_as_of_ms, 2_500);
    }

    #[test]
    fn universe_snapshot_does_not_rank_symbols_without_bootstrap() {
        let slices = vec![
            slice("upbit", "BTC", "KRW-BTC", 100.0, 2.0, "complete"),
            slice("upbit", "ETH", "KRW-ETH", 100.0, 5.0, "complete"),
        ];

        let snapshot = build_symbol_universe_snapshot(
            "run-1",
            InputRange {
                start_ms: 1_000,
                end_ms: 2_000,
            },
            2_500,
            &slices,
        );

        assert!(snapshot.included_symbols.is_empty());
        assert_eq!(snapshot.excluded_symbols.len(), 2);
        assert!(snapshot.liquidity_rank_at_that_time.is_empty());
        assert!(
            snapshot
                .excluded_symbols
                .iter()
                .all(|row| !row.approved_universe_symbol)
        );
    }

    #[test]
    fn feature_delta_uses_lookback_projection_slices() {
        let slices = vec![
            slice_at("binance", "SUI", "SUIUSDT", 10.0, 100.0, "complete", 0),
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                20.0,
                105.0,
                "complete",
                ONE_HOUR_MS - FIFTEEN_MINUTES_MS,
            ),
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                30.0,
                110.0,
                "complete",
                ONE_HOUR_MS,
            ),
        ];

        let deltas = build_market_feature_deltas(
            "run-1",
            InputRange {
                start_ms: ONE_HOUR_MS,
                end_ms: ONE_HOUR_MS + 1_000,
            },
            ONE_HOUR_MS + 1_500,
            &slices,
            &[],
        );

        let price_delta = deltas
            .iter()
            .find(|delta| delta.metric_name == "price")
            .expect("price delta exists");
        assert_eq!(price_delta.value_15m_ago, Some(105.0));
        assert_eq!(price_delta.value_1h_ago, Some(100.0));
        assert_eq!(price_delta.quality_status, "complete");
        assert!(price_delta.change_pct_1h.is_some_and(|value| value > 9.9));
    }

    #[test]
    fn feature_delta_uses_derivative_metric_history_without_spot_slices() {
        let derivative_metrics = vec![
            derivative_metric("open_interest", "BTCUSDT", 10_000.0, 0),
            derivative_metric(
                "open_interest",
                "BTCUSDT",
                11_000.0,
                ONE_HOUR_MS - FIFTEEN_MINUTES_MS,
            ),
            derivative_metric("open_interest", "BTCUSDT", 12_000.0, ONE_HOUR_MS),
        ];

        let deltas = build_market_feature_deltas(
            "run-1",
            InputRange {
                start_ms: ONE_HOUR_MS,
                end_ms: ONE_HOUR_MS + 1_000,
            },
            ONE_HOUR_MS + 1_500,
            &[],
            &derivative_metrics,
        );

        assert_eq!(deltas.len(), 1);
        let delta = &deltas[0];
        assert_eq!(delta.metric_name, "open_interest");
        assert_eq!(delta.market_type, "usdm_perpetual");
        assert_eq!(delta.value_15m_ago, Some(11_000.0));
        assert_eq!(delta.value_1h_ago, Some(10_000.0));
        assert_eq!(delta.quality_status, "complete");
        assert_eq!(delta.known_as_of_ms, ONE_HOUR_MS + 250);
        assert!(delta.change_pct_15m.is_some_and(|value| value > 9.0));
    }

    #[test]
    fn feature_delta_summary_keeps_latest_metric_per_symbol() {
        let slices = vec![
            slice_at("binance", "SUI", "SUIUSDT", 10.0, 100.0, "complete", 0),
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                20.0,
                105.0,
                "complete",
                ONE_HOUR_MS - FIFTEEN_MINUTES_MS,
            ),
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                30.0,
                110.0,
                "complete",
                ONE_HOUR_MS,
            ),
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                40.0,
                112.0,
                "complete",
                ONE_HOUR_MS + 1_000,
            ),
        ];
        let deltas = build_market_feature_deltas(
            "run-1",
            InputRange {
                start_ms: ONE_HOUR_MS,
                end_ms: ONE_HOUR_MS + 2_000,
            },
            ONE_HOUR_MS + 2_500,
            &slices,
            &[],
        );

        assert!(deltas.len() > 2);
        let summary = build_market_feature_delta_summary(
            "run-1",
            InputRange {
                start_ms: ONE_HOUR_MS,
                end_ms: ONE_HOUR_MS + 2_000,
            },
            ONE_HOUR_MS + 2_500,
            "market_feature_delta/run_id=run-1/delta.json",
            &deltas,
        );

        assert_eq!(summary.schema_version, "market_feature_delta_summary_v1");
        assert_eq!(summary.detail_record_count, deltas.len());
        assert_eq!(summary.summary_row_count, 1);
        assert_eq!(summary.rows[0].metrics.len(), 2);
        let price_metric = summary.rows[0]
            .metrics
            .iter()
            .find(|metric| metric.metric_name == "price")
            .expect("price metric exists");
        assert_eq!(price_metric.window_start_ms, ONE_HOUR_MS + 1_000);
        assert_eq!(price_metric.value_now, 112.0);
    }

    #[test]
    fn regime_context_separates_market_wide_returns() {
        let slices = vec![
            slice_at("binance", "BTC", "BTCUSDT", 1.0, 100.0, "complete", 0),
            slice_at("binance", "ETH", "ETHUSDT", 1.0, 200.0, "complete", 0),
            slice_at("binance", "SUI", "SUIUSDT", 1.0, 10.0, "complete", 0),
            slice_at(
                "binance",
                "BTC",
                "BTCUSDT",
                1.0,
                101.0,
                "complete",
                ONE_HOUR_MS,
            ),
            slice_at(
                "binance",
                "ETH",
                "ETHUSDT",
                1.0,
                204.0,
                "complete",
                ONE_HOUR_MS,
            ),
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                1.0,
                11.0,
                "complete",
                ONE_HOUR_MS,
            ),
        ];

        let contexts = build_market_regime_contexts(
            "run-1",
            InputRange {
                start_ms: ONE_HOUR_MS,
                end_ms: ONE_HOUR_MS + 1_000,
            },
            ONE_HOUR_MS + 1_500,
            &slices,
        );

        let context = contexts.first().expect("regime context exists");
        assert!(context.btc_return_same_window.is_some());
        assert!(context.eth_return_same_window.is_some());
        assert!(context.sector_return_same_window.is_some());
        assert_ne!(context.volatility_regime, "unknown");
    }

    #[test]
    fn regime_context_uses_degraded_prior_return_when_one_hour_history_missing() {
        let slices = vec![
            slice_at("binance", "BTC", "BTCUSDT", 1.0, 100.0, "complete", 0),
            slice_at("binance", "ETH", "ETHUSDT", 1.0, 200.0, "complete", 0),
            slice_at("binance", "SUI", "SUIUSDT", 1.0, 10.0, "complete", 0),
            slice_at("binance", "BTC", "BTCUSDT", 1.0, 101.0, "complete", 300_000),
            slice_at("binance", "ETH", "ETHUSDT", 1.0, 204.0, "complete", 300_000),
            slice_at("binance", "SUI", "SUIUSDT", 1.0, 11.0, "complete", 300_000),
        ];

        let contexts = build_market_regime_contexts(
            "run-1",
            InputRange {
                start_ms: 300_000,
                end_ms: 301_000,
            },
            301_500,
            &slices,
        );

        let context = contexts.first().expect("short-lookback context exists");
        assert!(context.btc_return_same_window.is_some());
        assert!(context.eth_return_same_window.is_some());
        assert!(context.sector_return_same_window.is_some());
        assert_eq!(context.quality_status, "partial");
        assert!(
            context
                .missing_reasons
                .contains(&"return_lookback_degraded".to_owned())
        );
    }

    #[test]
    fn universe_snapshot_approves_top_liquid_symbol_after_actual_30d_bootstrap() {
        let slices = (0..30)
            .map(|day| {
                slice_at(
                    "binance",
                    "SUI",
                    "SUIUSDT",
                    1_000.0,
                    10.0,
                    "complete",
                    day * 86_400_000,
                )
            })
            .collect::<Vec<_>>();

        let snapshot = build_symbol_universe_snapshot(
            "run-1",
            InputRange {
                start_ms: 0,
                end_ms: 30 * 86_400_000,
            },
            30 * 86_400_000,
            &slices,
        );

        assert_eq!(snapshot.included_symbols.len(), 1);
        assert_eq!(snapshot.included_symbols[0].symbol_canonical, "SUI");
        assert!(snapshot.included_symbols[0].approved_universe_symbol);
        assert_eq!(snapshot.included_symbols[0].bootstrap_days_available, 30);
    }

    #[test]
    fn universe_snapshot_approves_reference_warmup_symbol() {
        let rollups = vec![
            build_symbol_universe_bootstrap_rollups(
                "btc-warmup-run",
                InputRange {
                    start_ms: 0,
                    end_ms: 900_000,
                },
                900_000,
                &[slice_at(
                    "binance", "BTC", "BTCUSDT", 1_000.0, 100.0, "complete", 0,
                )],
            )
            .remove(0),
        ];
        let current_slices = vec![slice_at(
            "binance", "BTC", "BTCUSDT", 1_000.0, 100.0, "complete", 0,
        )];

        let snapshot = build_symbol_universe_snapshot_from_bootstrap(
            "run-current",
            InputRange {
                start_ms: 0,
                end_ms: 900_000,
            },
            900_000,
            &current_slices,
            &rollups,
        );

        assert_eq!(snapshot.included_symbols.len(), 1);
        assert_eq!(snapshot.included_symbols[0].symbol_canonical, "BTC");
        assert!(snapshot.included_symbols[0].approved_universe_symbol);
        assert_eq!(snapshot.included_symbols[0].bootstrap_days_available, 1);
        assert_eq!(
            snapshot.included_symbols[0].status_reason,
            "approved_reference_warmup"
        );
    }

    #[test]
    fn universe_snapshot_uses_small_daily_bootstrap_rollups() {
        let rollups = (0..30)
            .map(|day| {
                build_symbol_universe_bootstrap_rollups(
                    &format!("run-{day}"),
                    InputRange {
                        start_ms: day * ONE_DAY_MS,
                        end_ms: day * ONE_DAY_MS + 900_000,
                    },
                    day * ONE_DAY_MS + 900_000,
                    &[slice_at(
                        "binance",
                        "SUI",
                        "SUIUSDT",
                        1_000.0,
                        10.0,
                        "complete",
                        day * ONE_DAY_MS,
                    )],
                )
                .remove(0)
            })
            .collect::<Vec<_>>();

        let current_slices = vec![slice_at(
            "binance",
            "SUI",
            "SUIUSDT",
            1_000.0,
            10.0,
            "complete",
            29 * ONE_DAY_MS,
        )];
        let snapshot = build_symbol_universe_snapshot_from_bootstrap(
            "run-current",
            InputRange {
                start_ms: 29 * ONE_DAY_MS,
                end_ms: 29 * ONE_DAY_MS + 900_000,
            },
            29 * ONE_DAY_MS + 900_000,
            &current_slices,
            &rollups,
        );

        assert_eq!(snapshot.included_symbols.len(), 1);
        assert_eq!(snapshot.included_symbols[0].symbol_canonical, "SUI");
        assert_eq!(snapshot.included_symbols[0].bootstrap_days_available, 30);
    }

    #[test]
    fn universe_snapshot_rank_ignores_current_noise_without_30d_bootstrap() {
        let mut rollups = (0..30)
            .map(|day| {
                build_symbol_universe_bootstrap_rollups(
                    &format!("sui-run-{day}"),
                    InputRange {
                        start_ms: day * ONE_DAY_MS,
                        end_ms: day * ONE_DAY_MS + 900_000,
                    },
                    day * ONE_DAY_MS + 900_000,
                    &[slice_at(
                        "binance",
                        "SUI",
                        "SUIUSDT",
                        1_000.0,
                        10.0,
                        "complete",
                        day * ONE_DAY_MS,
                    )],
                )
                .remove(0)
            })
            .collect::<Vec<_>>();
        rollups.push(
            build_symbol_universe_bootstrap_rollups(
                "pros-current-run",
                InputRange {
                    start_ms: 29 * ONE_DAY_MS,
                    end_ms: 29 * ONE_DAY_MS + 900_000,
                },
                29 * ONE_DAY_MS + 900_000,
                &[slice_at(
                    "upbit",
                    "PROS",
                    "KRW-PROS",
                    1_000_000.0,
                    100.0,
                    "complete",
                    29 * ONE_DAY_MS,
                )],
            )
            .remove(0),
        );

        let current_slices = vec![
            slice_at(
                "binance",
                "SUI",
                "SUIUSDT",
                100.0,
                10.0,
                "complete",
                29 * ONE_DAY_MS,
            ),
            slice_at(
                "upbit",
                "PROS",
                "KRW-PROS",
                1_000_000.0,
                100.0,
                "complete",
                29 * ONE_DAY_MS,
            ),
        ];
        let snapshot = build_symbol_universe_snapshot_from_bootstrap(
            "run-current",
            InputRange {
                start_ms: 29 * ONE_DAY_MS,
                end_ms: 29 * ONE_DAY_MS + 900_000,
            },
            29 * ONE_DAY_MS + 900_000,
            &current_slices,
            &rollups,
        );

        assert_eq!(snapshot.liquidity_rank_at_that_time.len(), 1);
        assert_eq!(
            snapshot.liquidity_rank_at_that_time[0].symbol_canonical,
            "SUI"
        );
        assert_eq!(snapshot.included_symbols.len(), 1);
        assert_eq!(snapshot.included_symbols[0].symbol_canonical, "SUI");
        let pros = snapshot
            .excluded_symbols
            .iter()
            .find(|row| row.symbol_canonical == "PROS")
            .expect("PROS remains excluded");
        assert_eq!(pros.bootstrap_days_available, 1);
        assert_eq!(pros.liquidity_rank_at_that_time, None);
        assert_eq!(pros.status_reason, "insufficient_30d_bootstrap");
    }

    #[test]
    fn bootstrap_rollup_merge_is_idempotent_for_same_source_window() {
        let current = build_symbol_universe_bootstrap_rollups(
            "run-1",
            InputRange {
                start_ms: 0,
                end_ms: 900_000,
            },
            900_000,
            &[slice_at(
                "binance", "SUI", "SUIUSDT", 1_000.0, 10.0, "complete", 0,
            )],
        )
        .remove(0);

        let merged_once = merge_symbol_universe_bootstrap_rollup(None, current.clone());
        let merged_twice =
            merge_symbol_universe_bootstrap_rollup(Some(merged_once.clone()), current);

        assert_eq!(merged_twice.source_windows.len(), 1);
        assert_eq!(
            merged_twice.symbols[0].traded_notional_sum,
            merged_once.symbols[0].traded_notional_sum
        );
    }

    fn slice(
        venue: &str,
        symbol_canonical: &str,
        symbol_native: &str,
        trade_volume: f64,
        price: f64,
        completeness: &str,
    ) -> SliceRow {
        slice_at(
            venue,
            symbol_canonical,
            symbol_native,
            trade_volume,
            price,
            completeness,
            1_000,
        )
    }

    fn slice_at(
        venue: &str,
        symbol_canonical: &str,
        symbol_native: &str,
        trade_volume: f64,
        price: f64,
        completeness: &str,
        window_start_ms: i64,
    ) -> SliceRow {
        SliceRow {
            slice_id: format!("{venue}-{symbol_canonical}"),
            venue: venue.to_owned(),
            source_role: "execution".to_owned(),
            symbol_native: symbol_native.to_owned(),
            symbol_canonical: symbol_canonical.to_owned(),
            base_asset: symbol_canonical.to_owned(),
            quote_asset: "USDT".to_owned(),
            market_type: "spot".to_owned(),
            window_ms: 1_000,
            window_start_ms,
            window_end_ms: window_start_ms + 1_000,
            slice_completeness: completeness.to_owned(),
            missing_reasons: Vec::new(),
            quality_ok: 1,
            quality_delayed: 0,
            quality_stale: 0,
            quality_gap: 0,
            quality_invalid: 0,
            trade_count: 1,
            trade_volume,
            last_trade_price: Some(price),
            last_trade_size: Some(trade_volume),
            best_bid: Some(price - 0.1),
            best_ask: Some(price + 0.1),
            mid_price: Some(price),
            spread_bps: Some(1.0),
            book_ticker_count: 1,
            depth_event_count: 0,
            depth_book_rebuilt: false,
            trade_events: Vec::new(),
            book_ticker_events: Vec::new(),
            depth_events: Vec::new(),
            ticker_events: Vec::new(),
            symbol_health_snapshot: None,
            source_health_snapshot: None,
            parent_event_ids: Vec::new(),
            parent_run_ids: Vec::new(),
        }
    }

    fn derivative_metric(
        metric_name: &str,
        symbol_native: &str,
        value: f64,
        exchange_timestamp_ms: i64,
    ) -> DerivativeMetricObservation {
        DerivativeMetricObservation {
            venue: "binance".to_owned(),
            source_role: "derivatives".to_owned(),
            market_type: "usdm_perpetual".to_owned(),
            metric_name: metric_name.to_owned(),
            symbol_native: symbol_native.to_owned(),
            symbol_canonical: symbol_native.trim_end_matches("USDT").to_owned(),
            base_asset: symbol_native.trim_end_matches("USDT").to_owned(),
            quote_asset: "USDT".to_owned(),
            value,
            unit: "contracts".to_owned(),
            exchange_timestamp_ms,
            ingest_timestamp_ms: exchange_timestamp_ms + 250,
            parent_event_id: format!("{metric_name}-{symbol_native}-{exchange_timestamp_ms}"),
            parent_run_id: "run-0".to_owned(),
        }
    }
}
