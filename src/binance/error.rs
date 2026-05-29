use crypto_market_data::MarketDataError;
use std::fmt;
use std::str::Utf8Error;
use tokio_tungstenite::tungstenite;

#[derive(Debug)]
pub enum BinanceIngestError {
    Http(reqwest::Error),
    MarketData(MarketDataError),
    Json(serde_json::Error),
    WebSocket(tungstenite::Error),
    Utf8(Utf8Error),
    Storage(String),
    InvalidMessage(String),
}

impl fmt::Display for BinanceIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MarketData(error) => write!(f, "binance market data error: {error}"),
            Self::Http(error) => write!(f, "binance http error: {error}"),
            Self::Json(error) => write!(f, "binance json error: {error}"),
            Self::WebSocket(error) => write!(f, "binance websocket error: {error}"),
            Self::Utf8(error) => write!(f, "binance utf-8 error: {error}"),
            Self::Storage(error) => write!(f, "binance storage error: {error}"),
            Self::InvalidMessage(message) => write!(f, "binance invalid message: {message}"),
        }
    }
}

impl std::error::Error for BinanceIngestError {}

impl From<MarketDataError> for BinanceIngestError {
    fn from(value: MarketDataError) -> Self {
        Self::MarketData(value)
    }
}

impl From<reqwest::Error> for BinanceIngestError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for BinanceIngestError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<tungstenite::Error> for BinanceIngestError {
    fn from(value: tungstenite::Error) -> Self {
        Self::WebSocket(value)
    }
}

impl From<Utf8Error> for BinanceIngestError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8(value)
    }
}
