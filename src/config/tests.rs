use super::types::SymbolConfig;
use super::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn loads_enabled_exchange_and_symbols_from_config_dir() {
    let dir = temp_config_dir("success");
    write_config_file(
        &dir,
        "universe.major-50.toml",
        r#"
            [[symbols]]
            exchange = "binance"
            base = "BTC"
            quote = "USDT"
            raw = "BTCUSDT"
            normalized = "BTC-USDT"
            enabled = true

            [[symbols]]
            exchange = "upbit"
            base = "ETH"
            quote = "KRW"
            raw = "KRW-ETH"
            normalized = "ETH-KRW"
            enabled = false
        "#,
    );
    write_config_file(&dir, "cost.paper.toml", "max_latency_ms = 250\n");
    write_config_file(
        &dir,
        "exchanges.toml",
        r#"
            [[exchanges]]
            id = "binance"
            enabled = true
            rest_base_url = "https://api.binance.com"
            websocket_url = "wss://stream.binance.com:9443/ws"
        "#,
    );

    let config = load_market_ingest_config(&dir).unwrap();
    let symbols = config.enabled_symbols_for_exchange("binance").unwrap();

    assert_eq!(config.max_latency_ms, 250);
    assert_eq!(config.enabled_exchange("binance").unwrap().id, "binance");
    assert_eq!(symbols.len(), 1);
    assert_eq!(symbols[0].normalized, "BTC-USDT");

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_enabled_exchange_without_tls_urls() {
    let dir = temp_config_dir("bad-exchange");
    write_config_file(&dir, "universe.major-50.toml", "symbols = []\n");
    write_config_file(&dir, "cost.paper.toml", "max_latency_ms = 250\n");
    write_config_file(
        &dir,
        "exchanges.toml",
        r#"
            [[exchanges]]
            id = "binance"
            enabled = true
            rest_base_url = "http://api.binance.com"
            websocket_url = "ws://stream.binance.com"
        "#,
    );

    let error = load_market_ingest_config(&dir).unwrap_err().to_string();

    assert!(error.contains("rest_base_url must use https"));
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_enabled_exchange_url_credentials() {
    let dir = temp_config_dir("credential-exchange");
    write_config_file(&dir, "universe.major-50.toml", "symbols = []\n");
    write_config_file(&dir, "cost.paper.toml", "max_latency_ms = 250\n");
    write_config_file(
        &dir,
        "exchanges.toml",
        r#"
            [[exchanges]]
            id = "binance"
            enabled = true
            rest_base_url = "https://user:secret@api.binance.com"
            websocket_url = "wss://stream.binance.com:9443/ws"
        "#,
    );

    let error = load_market_ingest_config(&dir).unwrap_err().to_string();

    assert!(error.contains("must not include credentials"));
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_enabled_exchange_url_query_or_fragment() {
    let dir = temp_config_dir("query-exchange");
    write_config_file(&dir, "universe.major-50.toml", "symbols = []\n");
    write_config_file(&dir, "cost.paper.toml", "max_latency_ms = 250\n");
    write_config_file(
        &dir,
        "exchanges.toml",
        r#"
            [[exchanges]]
            id = "binance"
            enabled = true
            rest_base_url = "https://api.binance.com?existing=query"
            websocket_url = "wss://stream.binance.com:9443/ws"
        "#,
    );

    let error = load_market_ingest_config(&dir).unwrap_err().to_string();

    assert!(error.contains("query or fragment"));
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn rejects_symbol_normalized_drift() {
    let config = MarketIngestFileConfig {
        max_latency_ms: 250,
        exchanges: Vec::new(),
        symbols: vec![SymbolConfig {
            exchange: "binance".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            raw: "BTCUSDT".to_owned(),
            normalized: "BTCUSD".to_owned(),
            enabled: true,
        }],
    };

    let error = config
        .enabled_symbols_for_exchange("binance")
        .unwrap_err()
        .to_string();

    assert!(error.contains("symbol normalized mismatch"));
}

fn temp_config_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "market-ingest-config-test-{label}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config_file(dir: &Path, name: &str, contents: &str) {
    fs::write(dir.join(name), contents).unwrap();
}
