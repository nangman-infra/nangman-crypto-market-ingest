use super::*;
use crate::storage::record::{RawMarketEventDraft, RawMarketEventRecord};

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
            payload_json:
                r#"{"type":"orderbook","orderbook_units":[{"bid_price":99.0,"ask_price":101.0}]}"#
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
