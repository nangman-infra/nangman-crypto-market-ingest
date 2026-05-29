use crypto_domain::Symbol;
use std::error::Error;

use super::types::{ExchangeSettings, MarketIngestFileConfig};

impl MarketIngestFileConfig {
    pub fn enabled_symbols_for_exchange(
        &self,
        exchange: &str,
    ) -> Result<Vec<Symbol>, Box<dyn Error>> {
        self.symbols
            .iter()
            .filter(|symbol| symbol.enabled && symbol.exchange == exchange)
            .map(|symbol| symbol.to_symbol())
            .collect()
    }

    pub fn enabled_exchange(&self, exchange_id: &str) -> Result<&ExchangeSettings, Box<dyn Error>> {
        self.exchanges
            .iter()
            .find(|exchange| exchange.enabled && exchange.id == exchange_id)
            .ok_or_else(|| format!("enabled {exchange_id} exchange config is required").into())
    }
}
