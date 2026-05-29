use super::super::common::{
    ONE_HOUR_MS, correlation, group_slices_by_symbol, mean, percent_change, price,
};
use crate::normalize::model::SliceRow;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(super) struct ReturnSample {
    pub(super) symbol_canonical: String,
    pub(super) window_end_ms: i64,
    pub(super) return_pct: f64,
    pub(super) lookback_ms: i64,
}

pub(super) fn return_for_symbol(samples: &[ReturnSample], symbol: &str) -> Option<f64> {
    samples
        .iter()
        .find(|sample| sample.symbol_canonical == symbol)
        .map(|sample| sample.return_pct)
}

pub(super) fn return_samples_by_window(slices: &[SliceRow]) -> BTreeMap<i64, Vec<ReturnSample>> {
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

pub(super) fn rolling_correlation_to_btc(
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
