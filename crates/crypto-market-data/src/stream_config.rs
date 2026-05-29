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
        binance_combined_stream_url(&self.base_url, &streams)
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

fn binance_combined_stream_url(
    base_url: &str,
    streams: &[String],
) -> Result<String, MarketDataError> {
    validate_combined_stream_names(streams)?;
    let base = reqwest::Url::parse(base_url.trim()).map_err(|error| {
        MarketDataError::InvalidMessage(format!("invalid Binance stream base URL: {error}"))
    })?;
    validate_binance_stream_base_url(&base)?;
    Ok(format!(
        "{}/stream?streams={}",
        base.as_str().trim_end_matches('/'),
        streams.join("/")
    ))
}

fn validate_binance_stream_base_url(base: &reqwest::Url) -> Result<(), MarketDataError> {
    if base.scheme() != "wss" {
        return Err(MarketDataError::InvalidMessage(
            "Binance stream base URL must use wss".to_owned(),
        ));
    }
    if base.host_str().is_none() {
        return Err(MarketDataError::InvalidMessage(
            "Binance stream base URL must include a host".to_owned(),
        ));
    }
    if !base.username().is_empty() || base.password().is_some() {
        return Err(MarketDataError::InvalidMessage(
            "Binance stream base URL must not include credentials".to_owned(),
        ));
    }
    if base.query().is_some() || base.fragment().is_some() {
        return Err(MarketDataError::InvalidMessage(
            "Binance stream base URL must not include query or fragment components".to_owned(),
        ));
    }
    Ok(())
}

fn validate_combined_stream_names(streams: &[String]) -> Result<(), MarketDataError> {
    for stream in streams {
        if stream.is_empty()
            || !stream
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'@')
        {
            return Err(MarketDataError::InvalidMessage(format!(
                "invalid Binance stream name: {stream}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BinanceStreamConfig, BinanceStreamKind};
    use crypto_domain::Symbol;

    fn config_with_url(base_url: &str) -> BinanceStreamConfig {
        BinanceStreamConfig::new(
            base_url,
            1_000,
            vec![Symbol::new("binance", "BTC", "USDT", "BTCUSDT").unwrap()],
        )
    }

    #[test]
    fn combined_stream_url_preserves_base_path_prefix() {
        let url = config_with_url("wss://proxy.example/binance/")
            .combined_stream_url(BinanceStreamKind::Ticker)
            .unwrap();

        assert_eq!(
            url,
            "wss://proxy.example/binance/stream?streams=btcusdt@ticker"
        );
    }

    #[test]
    fn combined_stream_url_rejects_non_wss_base_url() {
        let error = config_with_url("ws://stream.binance.com:9443")
            .combined_stream_url(BinanceStreamKind::Ticker)
            .unwrap_err()
            .to_string();

        assert!(error.contains("wss"));
    }

    #[test]
    fn combined_stream_url_rejects_credentials_in_base_url() {
        let error = config_with_url("wss://user:secret@stream.binance.com:9443")
            .combined_stream_url(BinanceStreamKind::Ticker)
            .unwrap_err()
            .to_string();

        assert!(error.contains("credentials"));
    }

    #[test]
    fn combined_stream_url_rejects_query_or_fragment_in_base_url() {
        for base_url in [
            "wss://stream.binance.com:9443?existing=query",
            "wss://stream.binance.com:9443#fragment",
        ] {
            let error = config_with_url(base_url)
                .combined_stream_url(BinanceStreamKind::Ticker)
                .unwrap_err()
                .to_string();

            assert!(error.contains("query or fragment"));
        }
    }

    #[test]
    fn combined_stream_url_rejects_stream_name_with_query_separator() {
        let config = BinanceStreamConfig::new(
            "wss://stream.binance.com:9443",
            1_000,
            vec![Symbol::new("binance", "BTC", "USDT", "BTCUSDT&limit=1").unwrap()],
        );
        let error = config
            .combined_stream_url(BinanceStreamKind::Ticker)
            .unwrap_err()
            .to_string();

        assert!(error.contains("invalid Binance stream name"));
    }
}
