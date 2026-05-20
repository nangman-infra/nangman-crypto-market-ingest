use crate::clock;
use serde::Serialize;
use serde_json::{Value, json};
use std::env;

const APP_NAME: &str = "market-ingest-app";
const SCHEMA_VERSION: &str = "market_ingest_log_v1";
const LOG_LEVEL_ENV: &str = "MARKET_INGEST_LOG_LEVEL";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

pub fn debug<T>(event: &str, fields: T) -> Result<(), serde_json::Error>
where
    T: Serialize,
{
    emit(Level::Debug, event, fields)
}

pub fn info<T>(event: &str, fields: T) -> Result<(), serde_json::Error>
where
    T: Serialize,
{
    emit(Level::Info, event, fields)
}

pub fn warn<T>(event: &str, fields: T) -> Result<(), serde_json::Error>
where
    T: Serialize,
{
    emit(Level::Warn, event, fields)
}

pub fn error<T>(event: &str, fields: T) -> Result<(), serde_json::Error>
where
    T: Serialize,
{
    emit(Level::Error, event, fields)
}

fn emit<T>(level: Level, event: &str, fields: T) -> Result<(), serde_json::Error>
where
    T: Serialize,
{
    if !should_emit(level) {
        return Ok(());
    }
    let output = serde_json::to_string(&envelope(level, event, serde_json::to_value(fields)?))?;
    match level {
        Level::Warn | Level::Error => eprintln!("{output}"),
        Level::Debug | Level::Info => println!("{output}"),
    }
    Ok(())
}

fn should_emit(level: Level) -> bool {
    level >= configured_min_level()
}

fn configured_min_level() -> Level {
    env::var(LOG_LEVEL_ENV)
        .ok()
        .as_deref()
        .and_then(parse_level)
        .unwrap_or(Level::Info)
}

fn parse_level(value: &str) -> Option<Level> {
    match value.trim().to_ascii_lowercase().as_str() {
        "debug" => Some(Level::Debug),
        "info" => Some(Level::Info),
        "warn" | "warning" => Some(Level::Warn),
        "error" => Some(Level::Error),
        _ => None,
    }
}

fn envelope(level: Level, event: &str, fields: Value) -> Value {
    json!({
        "schema_version": SCHEMA_VERSION,
        "app": APP_NAME,
        "level": level_name(level),
        "event": event,
        "timestamp_ms": clock::now_ms_u64(),
        "fields": fields
    })
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Debug => "debug",
        Level::Info => "info",
        Level::Warn => "warn",
        Level::Error => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_levels() {
        assert_eq!(parse_level("debug"), Some(Level::Debug));
        assert_eq!(parse_level("INFO"), Some(Level::Info));
        assert_eq!(parse_level("warning"), Some(Level::Warn));
        assert_eq!(parse_level("error"), Some(Level::Error));
        assert_eq!(parse_level("verbose"), None);
    }

    #[test]
    fn level_order_filters_lower_severity() {
        assert!(Level::Error >= Level::Info);
        assert!(Level::Warn >= Level::Info);
        assert!(Level::Debug < Level::Info);
    }

    #[test]
    fn envelope_keeps_contract_fields_and_nested_payload() {
        let output = envelope(Level::Warn, "source_degraded", json!({"venue":"binance"}));

        assert_eq!(output["schema_version"], SCHEMA_VERSION);
        assert_eq!(output["app"], APP_NAME);
        assert_eq!(output["level"], "warn");
        assert_eq!(output["event"], "source_degraded");
        assert_eq!(output["fields"]["venue"], "binance");
        assert!(output["timestamp_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn level_names_are_stable_lowercase_values() {
        assert_eq!(level_name(Level::Debug), "debug");
        assert_eq!(level_name(Level::Info), "info");
        assert_eq!(level_name(Level::Warn), "warn");
        assert_eq!(level_name(Level::Error), "error");
    }
}
