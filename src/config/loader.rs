use serde::Deserialize;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use super::types::{CostSettings, ExchangeSettingsFile, MarketIngestFileConfig, UniverseConfig};
use super::validation::{validate_exchanges, validate_max_latency};

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

fn read_toml<T: for<'de> Deserialize<'de>>(path: PathBuf) -> Result<T, Box<dyn Error>> {
    let raw = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    toml::from_str(&raw)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()).into())
}
