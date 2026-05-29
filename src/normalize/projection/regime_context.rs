mod quality;
mod returns;

use super::super::args::InputRange;
use super::super::model::{MARKET_REGIME_CONTEXT_SCHEMA_VERSION, MarketRegimeContext, SliceRow};
use super::common::{ONE_HOUR_MS, mean, population_stddev, stable_id};
use std::collections::BTreeMap;

use self::quality::{regime_missing_reasons, regime_quality_status, volatility_regime};
use self::returns::{
    ReturnSample, return_for_symbol, return_samples_by_window, rolling_correlation_to_btc,
};

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
