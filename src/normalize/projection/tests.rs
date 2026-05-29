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
fn universe_snapshot_rejects_reference_warmup_without_30d_bootstrap() {
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

    assert!(snapshot.included_symbols.is_empty());
    assert_eq!(snapshot.excluded_symbols.len(), 1);
    assert_eq!(snapshot.excluded_symbols[0].symbol_canonical, "BTC");
    assert!(!snapshot.excluded_symbols[0].approved_universe_symbol);
    assert_eq!(snapshot.excluded_symbols[0].bootstrap_days_available, 1);
    assert_eq!(
        snapshot.excluded_symbols[0].status_reason,
        "insufficient_30d_bootstrap"
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
fn universe_snapshot_reports_missing_spread_separately_from_wide_spread() {
    let mut rollups = (0..30)
        .map(|day| {
            let mut rollup = build_symbol_universe_bootstrap_rollups(
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
            .remove(0);
            for symbol in &mut rollup.symbols {
                symbol.spread_bps_median_samples.clear();
            }
            rollup
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

    assert!(snapshot.included_symbols.is_empty());
    assert_eq!(snapshot.excluded_symbols.len(), 1);
    assert_eq!(snapshot.excluded_symbols[0].bootstrap_days_available, 30);
    assert_eq!(snapshot.excluded_symbols[0].median_spread_bps_30d, None);
    assert_eq!(
        snapshot.excluded_symbols[0].status_reason,
        "missing_30d_spread"
    );
    for rollup in &mut rollups {
        for symbol in &mut rollup.symbols {
            symbol.spread_bps_median_samples = vec![MAX_MEDIAN_SPREAD_BPS + 1.0];
        }
    }
    let wide_snapshot = build_symbol_universe_snapshot_from_bootstrap(
        "run-current-wide",
        InputRange {
            start_ms: 29 * ONE_DAY_MS,
            end_ms: 29 * ONE_DAY_MS + 900_000,
        },
        29 * ONE_DAY_MS + 900_000,
        &current_slices,
        &rollups,
    );
    assert_eq!(
        wide_snapshot.excluded_symbols[0].status_reason,
        "spread_too_wide_30d"
    );
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
    let merged_twice = merge_symbol_universe_bootstrap_rollup(Some(merged_once.clone()), current);

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
