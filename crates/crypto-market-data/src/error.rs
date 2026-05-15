use crypto_domain::DomainError;
use std::fmt;
use tokio_tungstenite::tungstenite;

#[derive(Debug)]
pub enum MarketDataError {
    Json(serde_json::Error),
    Domain(DomainError),
    Http(reqwest::Error),
    WebSocket(tungstenite::Error),
    UnknownSymbol(String),
    InvalidMessage(String),
}

impl fmt::Display for MarketDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "json error: {error}"),
            Self::Domain(error) => write!(f, "domain error: {error}"),
            Self::Http(error) => write!(f, "http error: {error}"),
            Self::WebSocket(error) => write!(f, "websocket error: {error}"),
            Self::UnknownSymbol(symbol) => write!(f, "unknown symbol: {symbol}"),
            Self::InvalidMessage(message) => write!(f, "invalid market data message: {message}"),
        }
    }
}

impl std::error::Error for MarketDataError {}

impl From<serde_json::Error> for MarketDataError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<DomainError> for MarketDataError {
    fn from(value: DomainError) -> Self {
        Self::Domain(value)
    }
}

impl From<reqwest::Error> for MarketDataError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<tungstenite::Error> for MarketDataError {
    fn from(value: tungstenite::Error) -> Self {
        Self::WebSocket(value)
    }
}
