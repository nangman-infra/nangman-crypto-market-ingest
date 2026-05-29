use crate::storage::record::RawMarketEventRecord;
use serde_json::Value;

#[derive(Default)]
pub(super) struct QuoteFields {
    pub(super) price_source: String,
    pub(super) last_price: Option<f64>,
    pub(super) best_bid_price: Option<f64>,
    pub(super) best_ask_price: Option<f64>,
    pub(super) trade_volume: Option<f64>,
}

pub(super) fn quote_from_payload(
    record: &RawMarketEventRecord,
    payload: &Value,
) -> Option<QuoteFields> {
    match record.venue.as_str() {
        "binance" => quote_from_binance_payload(&record.event_type, payload),
        "upbit" => quote_from_upbit_payload(&record.event_type, payload),
        _ => None,
    }
}

pub(super) fn midpoint(bid: Option<f64>, ask: Option<f64>) -> Option<f64> {
    match (bid, ask) {
        (Some(bid), Some(ask)) if ask >= bid => Some((bid + ask) / 2.0),
        (Some(bid), None) => Some(bid),
        (None, Some(ask)) => Some(ask),
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
