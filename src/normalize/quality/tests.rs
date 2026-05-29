use super::super::args::InputRange;
use super::super::model::{BookTickerNormalized, GapAlertInput, SliceRow, TradeNormalized};
use super::{apply_health_and_gaps, finalize_slices};

#[test]
fn finalizes_depth_top_missing_when_top_book_absent() {
    let finalized = finalize_slices(vec![slice_row()].into_iter());
    let row = finalized.first().unwrap();
    assert!(
        row.missing_reasons
            .iter()
            .any(|reason| reason == "depth_top_missing")
    );
}

#[test]
fn prefixes_gap_alert_missing_reason() {
    let mut row = slice_row();
    let gaps = vec![GapAlertInput {
        venue: "binance".to_owned(),
        symbol_native: "BTCUSDT".to_owned(),
        gap_type: "ordering_violation".to_owned(),
        detected_at_ms: 1_000,
        payload_json: "{}".to_owned(),
        payload_sha256: "unused".to_owned(),
        schema_version: "gap_alert_v1".to_owned(),
    }];

    apply_health_and_gaps(
        &[],
        &[],
        &gaps,
        std::iter::once(&mut row),
        InputRange {
            start_ms: 1_000,
            end_ms: 2_000,
        },
    );

    assert_eq!(row.quality_gap, 1);
    assert!(
        row.missing_reasons
            .iter()
            .any(|reason| reason == "gap_alert.ordering_violation")
    );
}

#[test]
fn finalizes_event_order_before_last_value_fields() {
    let mut row = slice_row();
    row.trade_count = 2;
    row.trade_volume = 3.0;
    row.trade_events = vec![
        trade("late", 2_000, 12.0, 2.0),
        trade("early", 1_000, 10.0, 1.0),
    ];
    row.book_ticker_count = 2;
    row.book_ticker_events = vec![
        book("late-book", 2_000, 99.0, 101.0),
        book("early-book", 1_000, 90.0, 110.0),
    ];

    let finalized = finalize_slices(vec![row].into_iter());
    let row = finalized.first().unwrap();

    assert_eq!(row.trade_events[0].parent_event_id, "early");
    assert_eq!(row.trade_events[1].parent_event_id, "late");
    assert_eq!(row.last_trade_price, Some(12.0));
    assert_eq!(row.last_trade_size, Some(2.0));
    assert_eq!(row.book_ticker_events[0].parent_event_id, "early-book");
    assert_eq!(row.book_ticker_events[1].parent_event_id, "late-book");
    assert_eq!(row.best_bid, Some(99.0));
    assert_eq!(row.best_ask, Some(101.0));
}

fn slice_row() -> SliceRow {
    SliceRow {
        slice_id: "slice-1".to_owned(),
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        symbol_native: "BTCUSDT".to_owned(),
        symbol_canonical: "BTC".to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "USDT".to_owned(),
        market_type: "spot".to_owned(),
        window_ms: 1_000,
        window_start_ms: 1_000,
        window_end_ms: 2_000,
        slice_completeness: String::new(),
        missing_reasons: Vec::new(),
        quality_ok: 0,
        quality_delayed: 0,
        quality_stale: 0,
        quality_gap: 0,
        quality_invalid: 0,
        trade_count: 0,
        trade_volume: 0.0,
        last_trade_price: None,
        last_trade_size: None,
        best_bid: None,
        best_ask: None,
        mid_price: None,
        spread_bps: None,
        book_ticker_count: 0,
        depth_event_count: 0,
        depth_book_rebuilt: false,
        trade_events: Vec::new(),
        book_ticker_events: Vec::new(),
        depth_events: Vec::new(),
        ticker_events: Vec::new(),
        symbol_health_snapshot: None,
        source_health_snapshot: None,
        parent_event_ids: Vec::new(),
        parent_run_ids: Vec::new(),
    }
}

fn trade(id: &str, timestamp_ms: i64, price: f64, quantity: f64) -> TradeNormalized {
    TradeNormalized {
        exchange_timestamp_ms: timestamp_ms,
        ingest_timestamp_ms: timestamp_ms,
        price,
        quantity,
        side: "unknown".to_owned(),
        exchange_sequence: None,
        parent_event_id: id.to_owned(),
    }
}

fn book(id: &str, timestamp_ms: i64, best_bid: f64, best_ask: f64) -> BookTickerNormalized {
    BookTickerNormalized {
        exchange_timestamp_ms: timestamp_ms,
        ingest_timestamp_ms: timestamp_ms,
        best_bid,
        best_bid_qty: 1.0,
        best_ask,
        best_ask_qty: 1.0,
        exchange_sequence: None,
        parent_event_id: id.to_owned(),
    }
}
