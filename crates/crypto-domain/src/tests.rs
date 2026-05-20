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
fn ord_orders_by_numeric_value_across_scales() {
    let ninety_nine_point_nine = FixedDecimal::parse_unsigned("99.9").unwrap();
    let hundred_point_five = FixedDecimal::parse_unsigned("100.5").unwrap();
    // String lexicographic order would put "100.5" before "99.9". Verify
    // FixedDecimal Ord respects numeric order so BTreeMap<FixedDecimal, _>
    // gives a price-sorted iteration order.
    assert!(ninety_nine_point_nine < hundred_point_five);
}

#[test]
fn ord_treats_equal_values_at_different_scales_as_equal() {
    let one = FixedDecimal::parse_unsigned("1").unwrap();
    let one_padded = FixedDecimal::parse_unsigned("1.00000000").unwrap();
    assert_eq!(one.cmp(&one_padded), std::cmp::Ordering::Equal);
}

#[test]
fn ord_remains_total_under_align_overflow() {
    // i128::MAX aligned by another factor of 10 would overflow; Ord should
    // still produce a deterministic answer.
    let large = FixedDecimal::new(i128::MAX, 0);
    let small_with_higher_scale = FixedDecimal::new(1, 8);
    assert!(large > small_with_higher_scale);
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
