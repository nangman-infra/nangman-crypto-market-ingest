use crate::error::MarketDataError;
use crypto_domain::{MarketDepthSnapshot, MarketSnapshot, Symbol};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinanceStreamKind {
    Trade,
    Ticker,
    BookTicker,
    DiffDepth100ms,
    PartialDepth5,
    PartialDepth10,
    PartialDepth20,
}

impl BinanceStreamKind {
    fn suffix(self) -> &'static str {
        match self {
            Self::Trade => "trade",
            Self::Ticker => "ticker",
            Self::BookTicker => "bookTicker",
            Self::DiffDepth100ms => "depth@100ms",
            Self::PartialDepth5 => "depth5",
            Self::PartialDepth10 => "depth10",
            Self::PartialDepth20 => "depth20",
        }
    }

    pub fn name(self) -> &'static str {
        self.suffix()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceNormalizedMarketEvent {
    Market(MarketSnapshot),
    Depth(MarketDepthSnapshot),
}

#[derive(Debug, Clone)]
pub struct BinanceStreamConfig {
    pub base_url: String,
    pub max_latency_ms: i64,
    symbols_by_raw: HashMap<String, Symbol>,
}

impl BinanceStreamConfig {
    pub fn new(base_url: impl Into<String>, max_latency_ms: i64, symbols: Vec<Symbol>) -> Self {
        let symbols_by_raw = symbols
            .into_iter()
            .map(|symbol| (symbol.raw.to_ascii_uppercase(), symbol))
            .collect();
        Self {
            base_url: base_url.into(),
            max_latency_ms,
            symbols_by_raw,
        }
    }

    pub fn combined_stream_url(&self, kind: BinanceStreamKind) -> Result<String, MarketDataError> {
        self.combined_stream_url_for_kinds(&[kind])
    }

    pub fn combined_stream_url_for_kinds(
        &self,
        kinds: &[BinanceStreamKind],
    ) -> Result<String, MarketDataError> {
        let streams = self.combined_stream_names_for_kinds(kinds)?;
        Ok(format!(
            "{}/stream?streams={}",
            self.base_url.trim_end_matches('/'),
            streams.join("/")
        ))
    }

    pub fn combined_stream_names_for_kinds(
        &self,
        kinds: &[BinanceStreamKind],
    ) -> Result<Vec<String>, MarketDataError> {
        if self.symbols_by_raw.is_empty() {
            return Err(MarketDataError::InvalidMessage(
                "at least one symbol is required".to_owned(),
            ));
        }
        if kinds.is_empty() {
            return Err(MarketDataError::InvalidMessage(
                "at least one stream kind is required".to_owned(),
            ));
        }

        let mut raw_symbols = self.symbols_by_raw.keys().cloned().collect::<Vec<_>>();
        raw_symbols.sort();
        Ok(raw_symbols
            .iter()
            .flat_map(|symbol| {
                kinds
                    .iter()
                    .map(move |kind| format!("{}@{}", symbol.to_ascii_lowercase(), kind.suffix()))
            })
            .collect::<Vec<_>>())
    }

    pub(crate) fn symbol(&self, raw_symbol: &str) -> Result<Symbol, MarketDataError> {
        self.symbols_by_raw
            .get(&raw_symbol.to_ascii_uppercase())
            .cloned()
            .ok_or_else(|| MarketDataError::UnknownSymbol(raw_symbol.to_owned()))
    }
}
