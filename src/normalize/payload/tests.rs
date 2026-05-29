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
