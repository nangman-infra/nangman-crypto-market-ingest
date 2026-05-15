use super::*;

#[test]
fn parses_and_compares_fixed_decimal_across_scales() {
    let one = FixedDecimal::parse_unsigned("1").unwrap();
    let one_point_zero = FixedDecimal::parse_unsigned("1.00").unwrap();
    let smaller = FixedDecimal::parse_unsigned("0.99").unwrap();

    assert!(one.checked_eq(one_point_zero).unwrap());
    assert!(smaller.checked_lt(one).unwrap());
}

#[test]
fn divides_notional_by_price_to_quantity_scale() {
    let notional = FixedDecimal::parse_unsigned("100000.00").unwrap();
    let price = FixedDecimal::parse_unsigned("50000.00").unwrap();

    assert_eq!(
        notional.div_to_scale(price, 8).unwrap(),
        FixedDecimal::new(200_000_000, 8)
    );
}

#[test]
fn multiplies_price_and_quantity_to_quote_notional_scale() {
    let price = FixedDecimal::parse_unsigned("50000.00").unwrap();
    let quantity = FixedDecimal::parse_unsigned("2.00000000").unwrap();

    assert_eq!(
        price.mul_to_scale(quantity, 8).unwrap(),
        FixedDecimal::new(10_000_000_000_000, 8)
    );
}

#[test]
fn rejects_invalid_market_snapshot() {
    let symbol = Symbol::new("binance", "BTC", "USDT", "BTCUSDT").unwrap();
    let snapshot = MarketSnapshot {
        decision_trace_id: 1,
        exchange: "binance".to_owned(),
        symbol,
        event_time_ms: 1,
        received_time_ms: 1,
        sequence: 1,
        quality: EventQuality::Ok,
        last_price: FixedDecimal::parse_unsigned("100").unwrap(),
        best_bid: FixedDecimal::parse_unsigned("101").unwrap(),
        best_ask: FixedDecimal::parse_unsigned("100").unwrap(),
        best_bid_qty: FixedDecimal::parse_unsigned("1").unwrap(),
        best_ask_qty: FixedDecimal::parse_unsigned("1").unwrap(),
        spread_bps: Bps::new(1),
    };

    assert!(snapshot.validate().is_err());
}

#[test]
fn serializes_fixed_decimal_value_as_string() {
    let decimal = FixedDecimal::parse_unsigned("123.45").unwrap();

    assert_eq!(
        serde_json::to_string(&decimal).unwrap(),
        r#"{"value":"12345","scale":2}"#
    );
}
