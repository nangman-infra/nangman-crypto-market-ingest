use crate::storage::record::RawMarketEventRecord;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

pub const MARKET_LIVE_TICK_SCHEMA_VERSION: &str = "market_live_tick_v1";
pub const DEFAULT_MARKET_LIVE_NATS_STREAM: &str = "MARKET_LIVE";
pub const DEFAULT_MARKET_LIVE_NATS_SUBJECT_PREFIX: &str = "market_live_tick.created";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LiveMarketNatsConfig {
    pub url: String,
    pub stream: String,
    pub subject_prefix: String,
    pub required: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MarketLiveTick {
    pub schema_version: String,
    pub event_id: String,
    pub producer_run_id: String,
    pub venue: String,
    pub source_role: String,
    pub market_type: String,
    pub event_type: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub latency_ms: i64,
    pub sequence_id: String,
    pub sequence_tag: String,
    pub price_source: String,
    pub last_price: Option<f64>,
    pub best_bid_price: Option<f64>,
    pub best_ask_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub trade_volume: Option<f64>,
    pub payload_sha256: String,
}

pub struct LiveMarketPublisher {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
    stream: String,
    subject_prefix: String,
}

impl MarketLiveTick {
    pub fn from_raw_market_event(record: &RawMarketEventRecord) -> Self {
        let payload = serde_json::from_str::<Value>(&record.payload_json).ok();
        let quote = payload
            .as_ref()
            .and_then(|value| quote_from_payload(record, value))
            .unwrap_or_default();
        let mark_price = quote
            .last_price
            .or_else(|| midpoint(quote.best_bid_price, quote.best_ask_price));
        Self {
            schema_version: MARKET_LIVE_TICK_SCHEMA_VERSION.to_owned(),
            event_id: record.event_id.clone(),
            producer_run_id: record.producer_run_id.clone(),
            venue: record.venue.clone(),
            source_role: record.source_role.clone(),
            market_type: record.market_type.clone(),
            event_type: record.event_type.clone(),
            symbol_native: record.symbol_native.clone(),
            symbol_canonical: record.symbol_canonical.clone(),
            base_asset: record.base_asset.clone(),
            quote_asset: record.quote_asset.clone(),
            exchange_timestamp_ms: record.exchange_timestamp_ms,
            ingest_timestamp_ms: record.ingest_timestamp_ms,
            latency_ms: record
                .ingest_timestamp_ms
                .saturating_sub(record.exchange_timestamp_ms),
            sequence_id: record.sequence_id.clone(),
            sequence_tag: record.sequence_tag.clone(),
            price_source: quote.price_source,
            last_price: quote.last_price,
            best_bid_price: quote.best_bid_price,
            best_ask_price: quote.best_ask_price,
            mark_price,
            trade_volume: quote.trade_volume,
            payload_sha256: record.payload_sha256.clone(),
        }
    }

    pub fn has_mark_price(&self) -> bool {
        self.mark_price
            .is_some_and(|value| value.is_finite() && value > 0.0)
    }
}

impl LiveMarketPublisher {
    pub async fn connect(config: &LiveMarketNatsConfig) -> Result<Self, Box<dyn Error>> {
        let client = async_nats::connect(&config.url).await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self {
            client,
            jetstream,
            stream: config.stream.clone(),
            subject_prefix: config.subject_prefix.clone(),
        })
    }

    pub async fn publish_tick(&self, tick: &MarketLiveTick) -> Result<(), Box<dyn Error>> {
        if !tick.has_mark_price() {
            return Ok(());
        }
        let bytes = Bytes::from(serde_json::to_vec(tick)?);
        let message = async_nats::jetstream::message::PublishMessage::build()
            .expected_stream(&self.stream)
            .message_id(&tick.event_id)
            .payload(bytes);
        let subject = format!(
            "{}.{}.{}",
            self.subject_prefix,
            subject_token(&tick.venue),
            subject_token(&tick.symbol_canonical)
        );
        let ack = self.jetstream.send_publish(subject, message).await?.await?;
        if ack.stream != self.stream {
            return Err(format!(
                "NATS JetStream ack stream mismatch: expected {}, got {}",
                self.stream, ack.stream
            )
            .into());
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), Box<dyn Error>> {
        self.client.flush().await?;
        Ok(())
    }
}

#[derive(Default)]
struct QuoteFields {
    price_source: String,
    last_price: Option<f64>,
    best_bid_price: Option<f64>,
    best_ask_price: Option<f64>,
    trade_volume: Option<f64>,
}

fn quote_from_payload(record: &RawMarketEventRecord, payload: &Value) -> Option<QuoteFields> {
    match record.venue.as_str() {
        "binance" => quote_from_binance_payload(&record.event_type, payload),
        "upbit" => quote_from_upbit_payload(&record.event_type, payload),
        _ => None,
    }
}

fn quote_from_binance_payload(event_type: &str, payload: &Value) -> Option<QuoteFields> {
    let data = payload.get("data").unwrap_or(payload);
    match event_type {
        "trade" => Some(QuoteFields {
            price_source: "trade".to_owned(),
            last_price: numeric_field(data, "p"),
            trade_volume: numeric_field(data, "q"),
            ..QuoteFields::default()
        }),
        "ticker" => Some(QuoteFields {
            price_source: "ticker_last".to_owned(),
            last_price: numeric_field(data, "c"),
            trade_volume: numeric_field(data, "v"),
            ..QuoteFields::default()
        }),
        "book_ticker" => Some(QuoteFields {
            price_source: "book_ticker_mid".to_owned(),
            best_bid_price: numeric_field(data, "b"),
            best_ask_price: numeric_field(data, "a"),
            ..QuoteFields::default()
        }),
        "depth_delta" | "depth_snapshot" => Some(QuoteFields {
            price_source: "depth_top_mid".to_owned(),
            best_bid_price: first_level_price(data, "b")
                .or_else(|| first_level_price(data, "bids")),
            best_ask_price: first_level_price(data, "a")
                .or_else(|| first_level_price(data, "asks")),
            ..QuoteFields::default()
        }),
        _ => None,
    }
}

fn quote_from_upbit_payload(event_type: &str, payload: &Value) -> Option<QuoteFields> {
    match event_type {
        "trade" => Some(QuoteFields {
            price_source: "trade".to_owned(),
            last_price: numeric_field(payload, "trade_price"),
            trade_volume: numeric_field(payload, "trade_volume"),
            best_bid_price: numeric_field(payload, "best_bid_price"),
            best_ask_price: numeric_field(payload, "best_ask_price"),
        }),
        "ticker" => Some(QuoteFields {
            price_source: "ticker_trade_price".to_owned(),
            last_price: numeric_field(payload, "trade_price"),
            trade_volume: numeric_field(payload, "acc_trade_volume_24h"),
            ..QuoteFields::default()
        }),
        "depth_snapshot" => {
            let top = payload
                .get("orderbook_units")
                .and_then(Value::as_array)
                .and_then(|items| items.first());
            Some(QuoteFields {
                price_source: "orderbook_top_mid".to_owned(),
                best_bid_price: top.and_then(|value| numeric_field(value, "bid_price")),
                best_ask_price: top.and_then(|value| numeric_field(value, "ask_price")),
                ..QuoteFields::default()
            })
        }
        _ => None,
    }
}

fn numeric_field(value: &Value, field: &str) -> Option<f64> {
    match value.get(field)? {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
    .filter(|value| value.is_finite() && *value > 0.0)
}

fn first_level_price(value: &Value, field: &str) -> Option<f64> {
    let first = value
        .get(field)
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())?;
    match first {
        Value::Array(items) => items.first().and_then(parse_number_value),
        Value::Object(_) => numeric_field(first, "price"),
        _ => None,
    }
}

fn parse_number_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
    .filter(|value| value.is_finite() && *value > 0.0)
}

fn midpoint(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    match (bid, ask) {
        (Some(bid), Some(ask)) if ask >= bid => Some((bid + ask) / 2.0),
        (Some(bid), None) => Some(bid),
        (None, Some(ask)) => Some(ask),
        _ => None,
    }
}

fn subject_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::RawMarketEventDraft;

    #[test]
    fn builds_binance_trade_tick_with_price() {
        let record = RawMarketEventRecord::from_draft(
            RawMarketEventDraft {
                event_type: "trade".to_owned(),
                venue: "binance".to_owned(),
                source_role: "reference".to_owned(),
                market_type: "spot".to_owned(),
                symbol_native: "BTCUSDT".to_owned(),
                symbol_canonical: "BTC".to_owned(),
                base_asset: "BTC".to_owned(),
                quote_asset: "USDT".to_owned(),
                exchange_timestamp_ms: 100,
                ingest_timestamp_ms: 110,
                sequence_id: "binance:trade:1".to_owned(),
                sequence_tag: "binance:trade:1".to_owned(),
                exchange_sequence: Some(1),
                diff_first_update_id: None,
                diff_final_update_id: None,
                is_snapshot: false,
                stream_type: "REALTIME".to_owned(),
                stream_phase: "realtime".to_owned(),
                payload_json: r#"{"stream":"btcusdt@trade","data":{"p":"100.5","q":"0.25"}}"#
                    .to_owned(),
            },
            "run-1",
            1,
        );

        let tick = MarketLiveTick::from_raw_market_event(&record);

        assert_eq!(tick.schema_version, MARKET_LIVE_TICK_SCHEMA_VERSION);
        assert_eq!(tick.mark_price, Some(100.5));
        assert_eq!(tick.trade_volume, Some(0.25));
        assert_eq!(tick.price_source, "trade");
        assert!(tick.has_mark_price());
    }

    #[test]
    fn builds_upbit_orderbook_mid_price() {
        let record = RawMarketEventRecord::from_draft(
            RawMarketEventDraft {
                event_type: "depth_snapshot".to_owned(),
                venue: "upbit".to_owned(),
                source_role: "execution".to_owned(),
                market_type: "spot".to_owned(),
                symbol_native: "KRW-BTC".to_owned(),
                symbol_canonical: "BTC".to_owned(),
                base_asset: "BTC".to_owned(),
                quote_asset: "KRW".to_owned(),
                exchange_timestamp_ms: 100,
                ingest_timestamp_ms: 110,
                sequence_id: "upbit:orderbook:1".to_owned(),
                sequence_tag: "upbit:orderbook:1".to_owned(),
                exchange_sequence: None,
                diff_first_update_id: None,
                diff_final_update_id: None,
                is_snapshot: true,
                stream_type: "REALTIME".to_owned(),
                stream_phase: "realtime".to_owned(),
                payload_json: r#"{"type":"orderbook","orderbook_units":[{"bid_price":99.0,"ask_price":101.0}]}"#
                    .to_owned(),
            },
            "run-1",
            1,
        );

        let tick = MarketLiveTick::from_raw_market_event(&record);

        assert_eq!(tick.best_bid_price, Some(99.0));
        assert_eq!(tick.best_ask_price, Some(101.0));
        assert_eq!(tick.mark_price, Some(100.0));
        assert_eq!(tick.price_source, "orderbook_top_mid");
    }
}
