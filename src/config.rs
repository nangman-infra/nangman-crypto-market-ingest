use crypto_domain::Symbol;
use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct MarketIngestFileConfig {
    pub max_latency_ms: i64,
    pub exchanges: Vec<ExchangeSettings>,
    symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeSettings {
    pub id: String,
    pub enabled: bool,
    pub rest_base_url: String,
    pub websocket_url: String,
}

#[derive(Debug, Deserialize)]
struct UniverseConfig {
    symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct SymbolConfig {
    exchange: String,
    base: String,
    quote: String,
    raw: String,
    normalized: String,
    enabled: bool,
}

#[derive(Debug, Deserialize)]
struct CostSettings {
    max_latency_ms: i64,
}

#[derive(Debug, Deserialize)]
struct ExchangeSettingsFile {
    exchanges: Vec<ExchangeSettings>,
}

pub fn load_market_ingest_config(
    config_dir: &Path,
) -> Result<MarketIngestFileConfig, Box<dyn Error>> {
    let universe = read_toml::<UniverseConfig>(config_dir.join("universe.major-50.toml"))?;
    let cost = read_toml::<CostSettings>(config_dir.join("cost.paper.toml"))?;
    let exchanges = read_toml::<ExchangeSettingsFile>(config_dir.join("exchanges.toml"))?;

    validate_max_latency(cost.max_latency_ms)?;
    validate_exchanges(&exchanges.exchanges)?;

    Ok(MarketIngestFileConfig {
        max_latency_ms: cost.max_latency_ms,
        exchanges: exchanges.exchanges,
        symbols: universe.symbols,
    })
}

impl MarketIngestFileConfig {
    pub fn enabled_symbols_for_exchange(
        &self,
        exchange: &str,
    ) -> Result<Vec<Symbol>, Box<dyn Error>> {
        self.symbols
            .iter()
            .filter(|symbol| symbol.enabled && symbol.exchange == exchange)
            .map(SymbolConfig::to_symbol)
            .collect()
    }

    pub fn enabled_exchange(&self, exchange_id: &str) -> Result<&ExchangeSettings, Box<dyn Error>> {
        self.exchanges
            .iter()
            .find(|exchange| exchange.enabled && exchange.id == exchange_id)
            .ok_or_else(|| format!("enabled {exchange_id} exchange config is required").into())
    }
}

impl SymbolConfig {
    fn to_symbol(&self) -> Result<Symbol, Box<dyn Error>> {
        let symbol = Symbol::new(&self.exchange, &self.base, &self.quote, &self.raw)?;
        if symbol.normalized != self.normalized {
            return Err(format!(
                "symbol normalized mismatch: expected {}, got {}",
                self.normalized, symbol.normalized
            )
            .into());
        }
        Ok(symbol)
    }
}

fn read_toml<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<T, Box<dyn Error>> {
    let raw = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    toml::from_str(&raw)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()).into())
}

fn validate_max_latency(max_latency_ms: i64) -> Result<(), Box<dyn Error>> {
    if max_latency_ms <= 0 {
        return Err("cost.paper.toml max_latency_ms must be positive".into());
    }
    Ok(())
}

fn validate_exchanges(exchanges: &[ExchangeSettings]) -> Result<(), Box<dyn Error>> {
    for exchange in exchanges {
        if exchange.enabled {
            if !exchange.rest_base_url.starts_with("https://") {
                return Err(
                    format!("exchange {} rest_base_url must use https", exchange.id).into(),
                );
            }
            if !exchange.websocket_url.starts_with("wss://") {
                return Err(format!("exchange {} websocket_url must use wss", exchange.id).into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
