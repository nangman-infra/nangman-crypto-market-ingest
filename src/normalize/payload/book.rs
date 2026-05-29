use serde_json::Value;

use super::common::{binance_data, number_from_value};
use crate::normalize::model::{BookTickerNormalized, RawInputEvent};

pub fn parse_book_ticker(event: &RawInputEvent) -> Option<BookTickerNormalized> {
    let value = serde_json::from_str::<Value>(&event.payload_json).ok()?;
    let data = binance_data(&value).unwrap_or(&value);
    match (event.venue.as_str(), event.event_type.as_str()) {
        ("binance", "book_ticker") => parse_binance_book_ticker(event, data),
        ("upbit", "depth_snapshot") => parse_upbit_orderbook_top(event, data),
        _ => None,
    }
}

fn parse_binance_book_ticker(event: &RawInputEvent, data: &Value) -> Option<BookTickerNormalized> {
    let best_bid = number_from_value(data.get("b")?)?;
    let best_bid_qty = number_from_value(data.get("B")?)?;
    let best_ask = number_from_value(data.get("a")?)?;
    let best_ask_qty = number_from_value(data.get("A")?)?;
    valid_book(event, best_bid, best_bid_qty, best_ask, best_ask_qty)
}

fn parse_upbit_orderbook_top(event: &RawInputEvent, data: &Value) -> Option<BookTickerNormalized> {
    let first = data.get("orderbook_units")?.as_array()?.first()?;
    let best_bid = number_from_value(first.get("bid_price")?)?;
    let best_bid_qty = number_from_value(first.get("bid_size")?)?;
    let best_ask = number_from_value(first.get("ask_price")?)?;
    let best_ask_qty = number_from_value(first.get("ask_size")?)?;
    valid_book(event, best_bid, best_bid_qty, best_ask, best_ask_qty)
}

fn valid_book(
    event: &RawInputEvent,
    best_bid: f64,
    best_bid_qty: f64,
    best_ask: f64,
    best_ask_qty: f64,
) -> Option<BookTickerNormalized> {
    if best_bid <= 0.0 || best_ask <= 0.0 || best_bid_qty < 0.0 || best_ask_qty < 0.0 {
        return None;
    }
    if best_bid > best_ask {
        return None;
    }
    Some(BookTickerNormalized {
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        best_bid,
        best_bid_qty,
        best_ask,
        best_ask_qty,
        exchange_sequence: event.exchange_sequence,
        parent_event_id: event.event_id.clone(),
    })
}
