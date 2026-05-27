use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind};
use market_ingest_app::args::{Args, Venue, parse_args, print_help};
use market_ingest_app::clock;
use market_ingest_app::config::load_market_ingest_config;
use market_ingest_app::live::LiveMarketNatsConfig;
use market_ingest_app::log_stream;
use market_ingest_app::storage::{
    EvictionConfig, L0StorageConfig, S3RetentionLoopEvents, UnsealedOrphanConfig,
    cleanup_invalid_unsealed_once, disk_used_pct, evict_once, l0_s3_retention_config,
    spawn_s3_retention_loop,
};
use market_ingest_app::{binance, upbit};
use serde_json::json;
use std::env;
use std::error::Error;
use std::path::Path;
use std::process;
use std::time::Duration;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        let _ = log_stream::error(
            "market_ingest_error",
            json!({ "message": error.to_string() }),
        );
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };

    log_unsealed_orphan_cleanup(&args);
    let eviction_handle = spawn_eviction_loop(&args);
    let retention_handle = spawn_l0_s3_retention_loop(&args);

    let result = match args.venue {
        Venue::Binance => run_binance(args).await,
        Venue::Upbit => run_upbit(args).await,
    };

    for handle in [eviction_handle, retention_handle].into_iter().flatten() {
        handle.abort();
        let _ = handle.await;
    }
    result
}

fn spawn_eviction_loop(args: &Args) -> Option<JoinHandle<()>> {
    let config = EvictionConfig {
        spool_root: args.l0_spool_root.clone(),
        high_water_pct: args.local_disk_high_water_pct,
        emergency_pct: args.local_disk_emergency_pct,
        safety_floor_secs: args.safety_floor_hours.saturating_mul(3_600),
    };
    let interval_secs = args.eviction_check_interval_secs;
    Some(tokio::spawn(async move {
        run_eviction_loop(config, interval_secs).await;
    }))
}

fn spawn_l0_s3_retention_loop(args: &Args) -> Option<JoinHandle<()>> {
    if !args.s3_retention_enabled {
        return None;
    }
    let bucket = args.l0_s3_bucket.clone()?;
    let config = l0_s3_retention_config(
        bucket,
        args.aws_region.clone(),
        args.aws_profile.clone(),
        args.s3_retention_days,
        args.s3_retention_max_deletes_per_run,
    );
    let interval_secs = args.s3_retention_check_interval_secs;
    Some(spawn_s3_retention_loop(
        "l0",
        config,
        interval_secs,
        S3RetentionLoopEvents {
            run_event: "market_ingest_s3_retention_run",
            error_event: "market_ingest_s3_retention_error",
        },
    ))
}

fn log_unsealed_orphan_cleanup(args: &Args) {
    let config = UnsealedOrphanConfig {
        spool_root: args.l0_spool_root.clone(),
        safety_floor_secs: args.safety_floor_hours.saturating_mul(3_600),
    };
    match cleanup_invalid_unsealed_once(&config, clock::now_ms()) {
        Ok(stats) => {
            let fields = json!({
                "spool_root": config.spool_root.display().to_string(),
                "scanned_unsealed_count": stats.scanned_unsealed_count,
                "recent_unsealed_count": stats.recent_unsealed_count,
                "valid_unsealed_count": stats.valid_unsealed_count,
                "invalid_unsealed_count": stats.invalid_unsealed_count,
                "quarantined_count": stats.quarantined_count,
                "quarantined_bytes": stats.quarantined_bytes,
                "quarantine_root": stats.quarantine_root
                    .map(|path| path.display().to_string())
            });
            let result = if stats.invalid_unsealed_count > 0 || stats.quarantined_count > 0 {
                log_stream::warn("market_ingest_unsealed_orphan_cleanup", fields)
            } else {
                log_stream::debug("market_ingest_unsealed_orphan_cleanup", fields)
            };
            let _ = result;
        }
        Err(error) => {
            let _ = log_stream::warn(
                "market_ingest_unsealed_orphan_cleanup_failed",
                json!({ "error": error.to_string() }),
            );
        }
    }
}

async fn run_eviction_loop(config: EvictionConfig, interval_secs: u64) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let now_ms = clock::now_ms();
        let probe_root = config.spool_root.clone();
        let probe = move || disk_probe(&probe_root);
        match evict_once(&config, now_ms, probe) {
            Ok(stats) => {
                let _ = log_stream::debug(
                    "market_ingest_eviction_heartbeat",
                    json!({
                        "spool_root": config.spool_root.display().to_string(),
                        "disk_used_pct_before": stats.disk_used_pct_before,
                        "disk_used_pct_after": stats.disk_used_pct_after,
                        "high_water_pct": config.high_water_pct,
                        "emergency_pct": config.emergency_pct,
                        "triggered": stats.triggered,
                        "candidate_count": stats.candidate_count
                    }),
                );
                if !stats.triggered {
                    continue;
                }
                let _ = log_stream::info(
                    "market_ingest_eviction_run",
                    json!({
                        "disk_used_pct_before": stats.disk_used_pct_before,
                        "disk_used_pct_after": stats.disk_used_pct_after,
                        "evicted_count": stats.evicted_count,
                        "evicted_bytes": stats.evicted_bytes,
                        "candidate_count": stats.candidate_count,
                        "emergency": stats.emergency
                    }),
                );
            }
            Err(error) => {
                let _ = log_stream::error(
                    "market_ingest_eviction_error",
                    json!({ "message": error.to_string() }),
                );
            }
        }
    }
}

fn disk_probe(spool_root: &Path) -> Result<u8, market_ingest_app::storage::StorageError> {
    disk_used_pct(spool_root).map_err(market_ingest_app::storage::StorageError::Io)
}

async fn run_binance(args: Args) -> Result<(), Box<dyn Error>> {
    let config = load_market_ingest_config(&args.config_dir)?;
    let symbols = config.enabled_symbols_for_exchange("binance")?;
    if symbols.len() != args.expect_symbol_count {
        return Err(format!(
            "expected {} enabled Binance symbols, found {} in {}",
            args.expect_symbol_count,
            symbols.len(),
            args.config_dir.display()
        )
        .into());
    }

    let exchange = config.enabled_exchange("binance")?;
    let websocket_base_url = binance_websocket_base_url(&exchange.websocket_url);
    let rest_base_url = exchange.rest_base_url.clone();
    let markets = symbols
        .iter()
        .map(|symbol| binance::BinanceMarket {
            raw_symbol: symbol.raw.clone(),
            base_asset: symbol.base.clone(),
            quote_asset: symbol.quote.clone(),
        })
        .collect::<Vec<_>>();
    let market_stream_config =
        BinanceStreamConfig::new(websocket_base_url, config.max_latency_ms, symbols);
    let stream_kinds = vec![
        BinanceStreamKind::Trade,
        BinanceStreamKind::BookTicker,
        BinanceStreamKind::Ticker,
        BinanceStreamKind::DiffDepth100ms,
    ];

    let l0_storage = l0_storage_config(&args, "binance");

    binance::run_binance_l0_smoke(binance::BinanceRunConfig {
        config_dir: args.config_dir.display().to_string(),
        rest_base_url,
        futures_rest_base_url: args.binance_futures_rest_base_url,
        derivative_snapshot_interval_seconds: args.binance_derivatives_snapshot_interval_seconds,
        stream_config: market_stream_config,
        markets,
        duration_seconds: args.duration_seconds,
        log_interval_seconds: args.log_interval_seconds,
        depth_snapshot_limit: args.depth_snapshot_limit,
        expect_symbol_count: args.expect_symbol_count,
        allow_partial_symbol_coverage: args.allow_partial_symbol_coverage,
        stream_kinds,
        l0_storage,
    })
    .await?;
    Ok(())
}

async fn run_upbit(args: Args) -> Result<(), Box<dyn Error>> {
    let config = load_market_ingest_config(&args.config_dir)?;
    let exchange = config.enabled_exchange("upbit")?;
    let rest_base_url = args
        .upbit_rest_base_url
        .clone()
        .unwrap_or_else(|| exchange.rest_base_url.clone());
    let websocket_url = args
        .upbit_websocket_url
        .clone()
        .unwrap_or_else(|| exchange.websocket_url.clone());
    let l0_storage = l0_storage_config(&args, "upbit");

    upbit::run_upbit_l0_smoke(upbit::UpbitRunConfig {
        rest_base_url,
        websocket_url,
        quote_currency: args.upbit_quote_currency,
        duration_seconds: args.duration_seconds,
        log_interval_seconds: args.log_interval_seconds,
        expect_symbol_count: args.expect_symbol_count,
        allow_partial_symbol_coverage: args.allow_partial_symbol_coverage,
        orderbook_unit: args.upbit_orderbook_unit,
        l0_storage,
    })
    .await?;
    Ok(())
}

fn binance_websocket_base_url(websocket_url: &str) -> String {
    websocket_url
        .trim_end_matches("/ws")
        .trim_end_matches('/')
        .to_owned()
}

fn l0_storage_config(args: &Args, venue: &str) -> Option<L0StorageConfig> {
    args.l0_s3_bucket.as_ref().map(|bucket| L0StorageConfig {
        bucket: bucket.clone(),
        region: args.aws_region.clone(),
        profile: args.aws_profile.clone(),
        spool_root: args.l0_spool_root.clone(),
        run_id: format!("market-ingest-{venue}-{}", clock::now_secs()),
        flush_records: args.l0_flush_records,
        shard_count: args.l0_shard_count,
        live_nats: args.live_nats_url.as_ref().map(|url| LiveMarketNatsConfig {
            url: url.clone(),
            stream: args.live_nats_stream.clone(),
            subject_prefix: args.live_nats_subject_prefix.clone(),
            required: args.live_nats_required,
        }),
    })
}
