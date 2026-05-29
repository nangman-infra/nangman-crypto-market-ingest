use serde_json::Value;

use super::common::{binance_data, number_from_value};
use crate::normalize::model::{RawInputEvent, TradeNormalized};

pub fn parse_trade(event: &RawInputEvent) -> Option<TradeNormalized> {
    let value = serde_json::from_str::<Value>(&event.payload_json).ok()?;
    let data = binance_data(&value).unwrap_or(&value);
    match event.venue.as_str() {
        "binance" => parse_binance_trade(event, data),
        "upbit" => parse_upbit_trade(event, data),
        _ => None,
    }
}

fn parse_binance_trade(event: &RawInputEvent, data: &Value) -> Option<TradeNormalized> {
    let price = number_from_value(data.get("p")?)?;
    let quantity = number_from_value(data.get("q")?)?;
    if price <= 0.0 || quantity <= 0.0 {
        return None;
    }
    let buyer_is_maker = data.get("m").and_then(Value::as_bool).unwrap_or(false);
    Some(TradeNormalized {
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        price,
        quantity,
        side: if buyer_is_maker { "sell" } else { "buy" }.to_owned(),
        exchange_sequence: event.exchange_sequence,
        parent_event_id: event.event_id.clone(),
    })
}

fn parse_upbit_trade(event: &RawInputEvent, data: &Value) -> Option<TradeNormalized> {
    let price = number_from_value(data.get("trade_price")?)?;
    let quantity = number_from_value(data.get("trade_volume")?)?;
    if price <= 0.0 || quantity <= 0.0 {
        return None;
    }
    let side = match data.get("ask_bid").and_then(Value::as_str) {
        Some("BID") => "buy",
        Some("ASK") => "sell",
        _ => return None,
    };
    Some(TradeNormalized {
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        price,
        quantity,
        side: side.to_owned(),
        exchange_sequence: event.exchange_sequence,
        parent_event_id: event.event_id.clone(),
    })
}
