use super::UpbitIngestError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitTickerMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub code: String,
    pub timestamp: i64,
    pub trade_timestamp: Option<i64>,
    pub trade_price: Option<f64>,
    pub acc_trade_price_24h: Option<f64>,
    pub acc_trade_volume_24h: Option<f64>,
    pub stream_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitTradeMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub code: String,
    pub timestamp: i64,
    pub trade_timestamp: i64,
    pub trade_price: f64,
    pub trade_volume: f64,
    pub ask_bid: String,
    pub sequential_id: i64,
    pub best_ask_price: Option<f64>,
    pub best_ask_size: Option<f64>,
    pub best_bid_price: Option<f64>,
    pub best_bid_size: Option<f64>,
    pub stream_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderbookMessage {
    #[serde(rename = "type")]
    pub event_type: String,
    pub code: String,
    pub timestamp: i64,
    pub total_ask_size: f64,
    pub total_bid_size: f64,
    #[serde(default)]
    pub orderbook_units: Vec<UpbitOrderbookUnit>,
    pub stream_type: Option<String>,
    pub level: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpbitOrderbookUnit {
    pub ask_price: f64,
    pub bid_price: f64,
    pub ask_size: f64,
    pub bid_size: f64,
}

#[derive(Debug, Clone, Serialize)]
pub enum UpbitParsedEvent {
    Ticker(UpbitTickerMessage),
    Trade(UpbitTradeMessage),
    Orderbook(UpbitOrderbookMessage),
    Status(String),
    Error { name: String, message: String },
    Unknown(Value),
}

#[derive(Debug, Clone, Serialize)]
pub struct UpbitParsedEnvelope {
    pub event: UpbitParsedEvent,
    pub payload_json: String,
}

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

impl UpbitParsedEvent {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Ticker(_) => "ticker",
            Self::Trade(_) => "trade",
            Self::Orderbook(_) => "orderbook",
            Self::Status(_) => "status",
            Self::Error { .. } => "error",
            Self::Unknown(_) => "unknown",
        }
    }

    pub fn symbol(&self) -> Option<&str> {
        match self {
            Self::Ticker(message) => Some(&message.code),
            Self::Trade(message) => Some(&message.code),
            Self::Orderbook(message) => Some(&message.code),
            Self::Status(_) | Self::Error { .. } | Self::Unknown(_) => None,
        }
    }

    pub fn exchange_timestamp_ms(&self) -> Option<i64> {
        match self {
            Self::Ticker(message) => Some(message.timestamp),
            Self::Trade(message) => Some(message.trade_timestamp),
            Self::Orderbook(message) => Some(message.timestamp),
            Self::Status(_) | Self::Error { .. } | Self::Unknown(_) => None,
        }
    }
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
        Some("ticker") => serde_json::from_value(value)
            .map(UpbitParsedEvent::Ticker)
            .map_err(UpbitIngestError::from),
        Some("trade") => serde_json::from_value(value)
            .map(UpbitParsedEvent::Trade)
            .map_err(UpbitIngestError::from),
        Some("orderbook") => serde_json::from_value(value)
            .map(UpbitParsedEvent::Orderbook)
            .map_err(UpbitIngestError::from),
        _ => Ok(UpbitParsedEvent::Unknown(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_status_error_and_unknown_payloads() {
        let envelopes = parse_upbit_payload(
            r#"[
                {"status":"UP"},
                {"error":{"name":"INVALID_AUTH","message":"bad token"}},
                {"type":"custom","code":"KRW-BTC"}
            ]"#,
        )
        .unwrap();

        assert_eq!(envelopes.len(), 3);
        assert!(matches!(envelopes[0].event, UpbitParsedEvent::Status(_)));
        assert!(matches!(envelopes[1].event, UpbitParsedEvent::Error { .. }));
        assert!(matches!(envelopes[2].event, UpbitParsedEvent::Unknown(_)));
        assert_eq!(envelopes[0].event.kind_name(), "status");
        assert_eq!(envelopes[1].event.symbol(), None);
    }

    #[test]
    fn parses_trade_and_orderbook_metadata() {
        let trade = parse_upbit_payload(
            r#"{
                "type":"trade",
                "code":"KRW-BTC",
                "timestamp":100,
                "trade_timestamp":90,
                "trade_price":100.5,
                "trade_volume":0.25,
                "ask_bid":"BID",
                "sequential_id":42,
                "stream_type":"REALTIME"
            }"#,
        )
        .unwrap()
        .remove(0);
        let orderbook = parse_upbit_payload(
            r#"{
                "type":"orderbook",
                "code":"KRW-BTC",
                "timestamp":120,
                "total_ask_size":1.0,
                "total_bid_size":2.0,
                "orderbook_units":[{"ask_price":101.0,"bid_price":100.0,"ask_size":1.5,"bid_size":2.5}],
                "stream_type":"SNAPSHOT"
            }"#,
        )
        .unwrap()
        .remove(0);

        assert_eq!(trade.event.kind_name(), "trade");
        assert_eq!(trade.event.symbol(), Some("KRW-BTC"));
        assert_eq!(trade.event.exchange_timestamp_ms(), Some(90));
        assert_eq!(orderbook.event.kind_name(), "orderbook");
        assert_eq!(orderbook.event.exchange_timestamp_ms(), Some(120));
    }
}
