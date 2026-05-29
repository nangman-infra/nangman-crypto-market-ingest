use crypto_domain::Symbol;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Clone)]
pub struct MarketIngestFileConfig {
    pub max_latency_ms: i64,
    pub exchanges: Vec<ExchangeSettings>,
    pub(super) symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeSettings {
    pub id: String,
    pub enabled: bool,
    pub rest_base_url: String,
    pub websocket_url: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct UniverseConfig {
    pub(super) symbols: Vec<SymbolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SymbolConfig {
    pub(super) exchange: String,
    pub(super) base: String,
    pub(super) quote: String,
    pub(super) raw: String,
    pub(super) normalized: String,
    pub(super) enabled: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct CostSettings {
    pub(super) max_latency_ms: i64,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExchangeSettingsFile {
    pub(super) exchanges: Vec<ExchangeSettings>,
}

impl SymbolConfig {
    pub(super) fn to_symbol(&self) -> Result<Symbol, Box<dyn Error>> {
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
