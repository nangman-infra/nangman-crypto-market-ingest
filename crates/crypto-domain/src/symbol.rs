use crate::{AssetCode, DomainError, ExchangeId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Symbol {
    pub exchange: ExchangeId,
    pub base: AssetCode,
    pub quote: AssetCode,
    pub normalized: String,
    pub raw: String,
}

impl Symbol {
    pub fn new(exchange: &str, base: &str, quote: &str, raw: &str) -> Result<Self, DomainError> {
        if exchange.trim().is_empty()
            || base.trim().is_empty()
            || quote.trim().is_empty()
            || raw.trim().is_empty()
        {
            return Err(DomainError::InvalidSymbol(format!(
                "{exchange}:{base}/{quote}:{raw}"
            )));
        }
        let base = base.trim().to_ascii_uppercase();
        let quote = quote.trim().to_ascii_uppercase();
        Ok(Self {
            exchange: exchange.trim().to_owned(),
            normalized: format!("{base}-{quote}"),
            base,
            quote,
            raw: raw.trim().to_owned(),
        })
    }
}
