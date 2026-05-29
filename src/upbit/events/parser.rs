use super::message::{UpbitOrderbookMessage, UpbitTickerMessage, UpbitTradeMessage};
use super::parsed::{UpbitParsedEnvelope, UpbitParsedEvent};
use crate::upbit::UpbitIngestError;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct UpbitMessageHeader {
    #[serde(rename = "type")]
    event_type: Option<String>,
    status: Option<String>,
    error: Option<UpbitErrorPayload>,
}

#[derive(Debug, Deserialize)]
struct UpbitErrorPayload {
    name: String,
    message: String,
}

pub fn parse_upbit_payload(raw_json: &str) -> Result<Vec<UpbitParsedEnvelope>, UpbitIngestError> {
    let value: Value = serde_json::from_str(raw_json)?;
    match value {
        Value::Array(items) => items.into_iter().map(parse_one_envelope).collect(),
        other => Ok(vec![parse_one_envelope(other)?]),
    }
}

fn parse_one_envelope(value: Value) -> Result<UpbitParsedEnvelope, UpbitIngestError> {
    let payload_json = serde_json::to_string(&value)?;
    Ok(UpbitParsedEnvelope {
        event: parse_one_value(value)?,
        payload_json,
    })
}

fn parse_one_value(value: Value) -> Result<UpbitParsedEvent, UpbitIngestError> {
    let header: UpbitMessageHeader = serde_json::from_value(value.clone())?;
    if let Some(status) = header.status {
        return Ok(UpbitParsedEvent::Status(status));
    }
    if let Some(error) = header.error {
        return Ok(UpbitParsedEvent::Error {
            name: error.name,
            message: error.message,
        });
    }

    match header.event_type.as_deref() {
        Some("ticker") => serde_json::from_value::<UpbitTickerMessage>(value)
            .map(UpbitParsedEvent::Ticker)
            .map_err(UpbitIngestError::from),
        Some("trade") => serde_json::from_value::<UpbitTradeMessage>(value)
            .map(UpbitParsedEvent::Trade)
            .map_err(UpbitIngestError::from),
        Some("orderbook") => serde_json::from_value::<UpbitOrderbookMessage>(value)
            .map(UpbitParsedEvent::Orderbook)
            .map_err(UpbitIngestError::from),
        _ => Ok(UpbitParsedEvent::Unknown(value)),
    }
}
