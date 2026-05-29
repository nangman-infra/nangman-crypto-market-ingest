use std::fmt;

#[derive(Debug)]
pub enum BackfillError {
    Http(reqwest::Error),
    Json(serde_json::Error),
    Storage(String),
    InvalidArgs(String),
    InvalidConfig(String),
}

impl fmt::Display for BackfillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => write!(f, "market backfill http error: {error}"),
            Self::Json(error) => write!(f, "market backfill json error: {error}"),
            Self::Storage(error) => write!(f, "market backfill storage error: {error}"),
            Self::InvalidArgs(error) => write!(f, "market backfill invalid args: {error}"),
            Self::InvalidConfig(error) => write!(f, "market backfill invalid config: {error}"),
        }
    }
}

impl std::error::Error for BackfillError {}

impl From<reqwest::Error> for BackfillError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for BackfillError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
