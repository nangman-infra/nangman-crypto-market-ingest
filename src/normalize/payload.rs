use super::model::{
    BookTickerNormalized, CompactEventRef, DerivativeMetricObservation, RawInputEvent,
    TradeNormalized,
};
use serde_json::Value;

pub fn parse_trade(event: &RawInputEvent) -> Option<TradeNormalized> {
    let value = serde_json::from_str::<Value>(&event.payload_json).ok()?;
    let data = binance_data(&value).unwrap_or(&value);
    match event.venue.as_str() {
        "binance" => parse_binance_trade(event, data),
        "upbit" => parse_upbit_trade(event, data),
        _ => None,
    }
}

pub fn parse_book_ticker(event: &RawInputEvent) -> Option<BookTickerNormalized> {
    let value = serde_json::from_str::<Value>(&event.payload_json).ok()?;
    let data = binance_data(&value).unwrap_or(&value);
    match (event.venue.as_str(), event.event_type.as_str()) {
        ("binance", "book_ticker") => parse_binance_book_ticker(event, data),
        ("upbit", "depth_snapshot") => parse_upbit_orderbook_top(event, data),
        _ => None,
    }
}

pub fn compact_ref(event: &RawInputEvent) -> CompactEventRef {
    CompactEventRef {
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        event_type: event.event_type.clone(),
        parent_event_id: event.event_id.clone(),
    }
}

pub fn parse_derivative_metric(event: &RawInputEvent) -> Option<DerivativeMetricObservation> {
    let value = serde_json::from_str::<Value>(&event.payload_json).ok()?;
    let (metric_name, metric_value, unit) = match (event.venue.as_str(), event.event_type.as_str())
    {
        ("binance", "funding_rate_snapshot") => (
            "funding_rate",
            number_from_value(
                value
                    .get("funding_rate")
                    .or_else(|| value.get("lastFundingRate"))?,
            )?,
            "ratio",
        ),
        ("binance", "open_interest_snapshot") => (
            "open_interest",
            number_from_value(
                value
                    .get("open_interest")
                    .or_else(|| value.get("openInterest"))?,
            )?,
            "contracts",
        ),
        _ => return None,
    };
    if !metric_value.is_finite() {
        return None;
    }
    Some(DerivativeMetricObservation {
        venue: event.venue.clone(),
        source_role: event.source_role.clone(),
        market_type: event.market_type.clone(),
        metric_name: metric_name.to_owned(),
        symbol_native: event.symbol_native.clone(),
        symbol_canonical: event.symbol_canonical.clone(),
        base_asset: event.base_asset.clone(),
        quote_asset: event.quote_asset.clone(),
        value: metric_value,
        unit: unit.to_owned(),
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        parent_event_id: event.event_id.clone(),
        parent_run_id: event.producer_run_id.clone(),
    })
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

fn binance_data(value: &Value) -> Option<&Value> {
    value.get("data")
}

fn number_from_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::model::RawInputEvent;

    #[test]
    fn parses_binance_taker_side() {
        let event = raw(
            "binance",
            "trade",
            r#"{"data":{"p":"10","q":"2","m":false}}"#,
        );
        let trade = parse_trade(&event).unwrap();
        assert_eq!(trade.side, "buy");
        assert_eq!(trade.price, 10.0);
    }

    #[test]
    fn derives_upbit_orderbook_top() {
        let event = raw(
            "upbit",
            "depth_snapshot",
            r#"{"orderbook_units":[{"bid_price":9.0,"bid_size":1.0,"ask_price":10.0,"ask_size":2.0}]}"#,
        );
        let book = parse_book_ticker(&event).unwrap();
        assert_eq!(book.best_bid, 9.0);
    }

    #[test]
    fn parses_binance_derivative_metrics_without_spot_slice_fields() {
        let funding = raw(
            "binance",
            "funding_rate_snapshot",
            r#"{"symbol":"BTCUSDT","funding_rate":"0.0001","unit":"ratio"}"#,
        );
        let funding_metric = parse_derivative_metric(&funding).unwrap();
        assert_eq!(funding_metric.metric_name, "funding_rate");
        assert_eq!(funding_metric.value, 0.0001);
        assert_eq!(funding_metric.unit, "ratio");

        let oi = raw(
            "binance",
            "open_interest_snapshot",
            r#"{"symbol":"BTCUSDT","open_interest":"12345.5","unit":"contracts"}"#,
        );
        let oi_metric = parse_derivative_metric(&oi).unwrap();
        assert_eq!(oi_metric.metric_name, "open_interest");
        assert_eq!(oi_metric.value, 12345.5);
        assert_eq!(oi_metric.unit, "contracts");
    }

    fn raw(venue: &str, event_type: &str, payload_json: &str) -> RawInputEvent {
        RawInputEvent {
            event_id: "event-1".to_owned(),
            producer_run_id: "run-1".to_owned(),
            venue: venue.to_owned(),
            source_role: "reference".to_owned(),
            market_type: "spot".to_owned(),
            event_type: event_type.to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            symbol_canonical: "BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "USDT".to_owned(),
            exchange_timestamp_ms: 1,
            ingest_timestamp_ms: 2,
            exchange_sequence: Some(3),
            payload_json: payload_json.to_owned(),
            payload_sha256: String::new(),
            schema_version: "raw_market_event_v2".to_owned(),
        }
    }
}
