use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind};
use market_ingest_app::args::Args;
use market_ingest_app::clock;
use market_ingest_app::config::load_market_ingest_config;
use market_ingest_app::live::LiveMarketNatsConfig;
use market_ingest_app::storage::L0StorageConfig;
use market_ingest_app::{binance, upbit};
use std::error::Error;

pub(crate) async fn run_binance(args: Args) -> Result<(), Box<dyn Error>> {
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
    let websocket_base_url = binance_websocket_base_url(&exchange.websocket_url)?;
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

pub(crate) async fn run_upbit(args: Args) -> Result<(), Box<dyn Error>> {
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

fn binance_websocket_base_url(websocket_url: &str) -> Result<String, Box<dyn Error>> {
    let mut url = reqwest::Url::parse(websocket_url.trim())
        .map_err(|error| format!("invalid Binance websocket URL: {error}"))?;
    validate_binance_websocket_url(&url)?;

    let trimmed_path = url.path().trim_end_matches('/').to_owned();
    if let Some(base_path) = trimmed_path.strip_suffix("/ws") {
        url.set_path(base_path);
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

fn validate_binance_websocket_url(url: &reqwest::Url) -> Result<(), Box<dyn Error>> {
    if url.scheme() != "wss" {
        return Err("Binance websocket URL must use wss".into());
    }
    if url.host_str().is_none() {
        return Err("Binance websocket URL must include a host".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("Binance websocket URL must not include credentials".into());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("Binance websocket URL must not include query or fragment components".into());
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::binance_websocket_base_url;

    #[test]
    fn binance_websocket_base_url_strips_raw_ws_suffix() {
        let base_url = binance_websocket_base_url("wss://stream.binance.com:9443/ws").unwrap();

        assert_eq!(base_url, "wss://stream.binance.com:9443");
    }

    #[test]
    fn binance_websocket_base_url_preserves_proxy_path_prefix() {
        let base_url = binance_websocket_base_url("wss://proxy.example/binance/ws").unwrap();

        assert_eq!(base_url, "wss://proxy.example/binance");
    }

    #[test]
    fn binance_websocket_base_url_accepts_base_endpoint() {
        let base_url = binance_websocket_base_url("wss://stream.binance.com:9443").unwrap();

        assert_eq!(base_url, "wss://stream.binance.com:9443");
    }

    #[test]
    fn binance_websocket_base_url_rejects_non_wss_url() {
        let error = binance_websocket_base_url("ws://stream.binance.com:9443/ws")
            .unwrap_err()
            .to_string();

        assert!(error.contains("wss"));
    }

    #[test]
    fn binance_websocket_base_url_rejects_credentials() {
        let error = binance_websocket_base_url("wss://user:secret@stream.binance.com:9443/ws")
            .unwrap_err()
            .to_string();

        assert!(error.contains("credentials"));
    }

    #[test]
    fn binance_websocket_base_url_rejects_query_or_fragment() {
        for websocket_url in [
            "wss://stream.binance.com:9443/ws?existing=query",
            "wss://stream.binance.com:9443/ws#fragment",
        ] {
            let error = binance_websocket_base_url(websocket_url)
                .unwrap_err()
                .to_string();

            assert!(error.contains("query or fragment"));
        }
    }
}
