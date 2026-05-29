use std::fmt;
use std::str::Utf8Error;
use tokio_tungstenite::tungstenite;

#[derive(Debug)]
pub enum UpbitIngestError {
    Http(reqwest::Error),
    Json(serde_json::Error),
    WebSocket(tungstenite::Error),
    Utf8(Utf8Error),
    Storage(String),
    InvalidConfig(String),
    InvalidMessage(String),
}

impl fmt::Display for UpbitIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => write!(f, "upbit http error: {error}"),
            Self::Json(error) => write!(f, "upbit json error: {error}"),
            Self::WebSocket(error) => write!(f, "upbit websocket error: {error}"),
            Self::Utf8(error) => write!(f, "upbit utf-8 error: {error}"),
            Self::Storage(error) => write!(f, "upbit storage error: {error}"),
            Self::InvalidConfig(message) => write!(f, "upbit invalid config: {message}"),
            Self::InvalidMessage(message) => write!(f, "upbit invalid message: {message}"),
        }
    }
}

impl std::error::Error for UpbitIngestError {}

impl From<reqwest::Error> for UpbitIngestError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for UpbitIngestError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<tungstenite::Error> for UpbitIngestError {
    fn from(value: tungstenite::Error) -> Self {
        Self::WebSocket(value)
    }
}

impl From<Utf8Error> for UpbitIngestError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8(value)
    }
}
