use super::*;
use std::collections::BTreeMap;

fn config() -> BinanceStreamConfig {
    BinanceStreamConfig::new(
        "wss://stream.binance.com:9443",
        1_000,
        vec![Symbol::new("binance", "BTC", "USDT", "BTCUSDT").unwrap()],
    )
}

#[test]
fn builds_combined_ticker_stream_url_with_lowercase_symbols() {
    let url = config()
        .combined_stream_url(BinanceStreamKind::Ticker)
        .unwrap();

    assert_eq!(
        url,
        "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker"
    );
}

#[test]
fn builds_combined_partial_depth_stream_url_with_lowercase_symbols() {
    let url = config()
        .combined_stream_url(BinanceStreamKind::PartialDepth5)
        .unwrap();

    assert_eq!(
        url,
        "wss://stream.binance.com:9443/stream?streams=btcusdt@depth5"
    );
}

#[test]
fn builds_combined_ticker_and_partial_depth_stream_url() {
    let url = config()
        .combined_stream_url_for_kinds(&[
            BinanceStreamKind::Ticker,
            BinanceStreamKind::PartialDepth5,
        ])
        .unwrap();

    assert_eq!(
        url,
        "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/btcusdt@depth5"
    );
}

#[test]
fn builds_combined_trade_book_ticker_and_depth_stream_names() {
    let streams = config()
        .combined_stream_names_for_kinds(&[
            BinanceStreamKind::Trade,
            BinanceStreamKind::BookTicker,
            BinanceStreamKind::DiffDepth100ms,
        ])
        .unwrap();

    assert_eq!(
        streams,
        vec!["btcusdt@trade", "btcusdt@bookTicker", "btcusdt@depth@100ms"]
    );
}

#[test]
fn normalizes_raw_binance_ticker_to_market_snapshot() {
    let snapshot = normalize_binance_ticker_message(
        &config(),
        r#"{
          "e": "24hrTicker",
          "E": 1672515782136,
          "s": "BTCUSDT",
          "c": "50000.00",
          "b": "49999.50",
          "B": "1.5",
          "a": "50000.50",
          "A": "2.0",
          "L": 18150
        }"#,
        1672515782256,
        42,
    )
    .unwrap();

    assert_eq!(snapshot.symbol.normalized, "BTC-USDT");
    assert_eq!(snapshot.quality, EventQuality::Ok);
    assert_eq!(snapshot.sequence, 18150);
    assert_eq!(snapshot.spread_bps, Bps::new(1));
}

#[test]
fn normalizes_combined_binance_ticker_to_market_snapshot() {
    let snapshot = normalize_binance_ticker_message(
        &config(),
        r#"{
          "stream": "btcusdt@ticker",
          "data": {
            "e": "24hrTicker",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "c": "50000.00",
            "b": "49950.00",
            "B": "1.5",
            "a": "50050.00",
            "A": "2.0",
            "L": 18151
          }
        }"#,
        1672515782256,
        43,
    )
    .unwrap();

    assert_eq!(snapshot.quality, EventQuality::Ok);
    assert_eq!(snapshot.spread_bps, Bps::new(20));
}

#[test]
fn normalizes_combined_binance_partial_depth_to_market_depth_snapshot() {
    let snapshot = normalize_binance_partial_depth_message(
        &config(),
        r#"{
          "stream": "btcusdt@depth5",
          "data": {
            "lastUpdateId": 160,
            "bids": [
              ["49999.50", "1.5"],
              ["49999.00", "0.5"]
            ],
            "asks": [
              ["50000.50", "2.0"],
              ["50001.00", "1.0"]
            ]
          }
        }"#,
        1672515782256,
        46,
    )
    .unwrap();

    assert_eq!(snapshot.symbol.normalized, "BTC-USDT");
    assert_eq!(snapshot.quality, EventQuality::Ok);
    assert_eq!(snapshot.sequence, 160);
    assert_eq!(snapshot.level_count, 2);
    assert_eq!(snapshot.bid_depth_qty, FixedDecimal::new(20, 1));
    assert_eq!(snapshot.ask_depth_qty, FixedDecimal::new(30, 1));
    assert_eq!(snapshot.depth_imbalance_bps, Bps::new(-2_000));
    assert_eq!(snapshot.spread_bps, Bps::new(1));
}

#[test]
fn normalizes_mixed_combined_stream_messages_for_public_replay() {
    let market_event = normalize_binance_stream_message(
        &config(),
        r#"{
          "stream": "btcusdt@ticker",
          "data": {
            "e": "24hrTicker",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "c": "50000.00",
            "b": "49999.50",
            "B": "1.5",
            "a": "50000.50",
            "A": "2.0",
            "L": 18151
          }
        }"#,
        1672515782256,
        49,
    )
    .unwrap();
    let depth_event = normalize_binance_stream_message(
        &config(),
        r#"{
          "stream": "btcusdt@depth5",
          "data": {
            "lastUpdateId": 160,
            "bids": [["49999.50", "1.5"]],
            "asks": [["50000.50", "2.0"]]
          }
        }"#,
        1672515782256,
        50,
    )
    .unwrap();

    assert!(matches!(
        market_event,
        BinanceNormalizedMarketEvent::Market(_)
    ));
    assert!(matches!(
        depth_event,
        BinanceNormalizedMarketEvent::Depth(_)
    ));
}

#[test]
fn observes_raw_ingest_trade_and_book_ticker_without_normalization() {
    let mut stats =
        BinanceIngestWatchStats::new("wss://stream.binance.com:9443/stream".to_owned(), 2);
    let mut last_sequence_by_stream = BTreeMap::new();

    observe_binance_ingest_payload(
        &config(),
        r#"{
          "stream": "btcusdt@trade",
          "data": {
            "e": "trade",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "t": 18152,
            "p": "50001.00",
            "q": "0.1",
            "T": 1672515782135,
            "m": false
          }
        }"#,
        1672515782256,
        51,
        &mut last_sequence_by_stream,
        &mut stats,
    );
    observe_binance_ingest_payload(
        &config(),
        r#"{
          "stream": "btcusdt@bookTicker",
          "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "50000.00",
            "B": "1.0",
            "a": "50001.00",
            "A": "2.0"
          }
        }"#,
        1672515782257,
        52,
        &mut last_sequence_by_stream,
        &mut stats,
    );

    assert_eq!(stats.received_messages, 2);
    assert_eq!(stats.parsed_messages, 2);
    assert_eq!(stats.kind_counts.get("trade"), Some(&1));
    assert_eq!(stats.kind_counts.get("bookTicker"), Some(&1));
    assert_eq!(stats.symbol_counts.get("BTCUSDT"), Some(&2));
    assert_eq!(stats.normalized_market_snapshots, 0);
    assert_eq!(stats.normalized_depth_snapshots, 0);
}

#[test]
fn syncs_diff_depth_buffer_from_rest_snapshot() {
    let mut book = BinanceLocalOrderBook::default();
    book.buffered_events.push(BinanceDiffDepthMessage {
        symbol: "BTCUSDT".to_owned(),
        first_update_id: 158,
        final_update_id: 160,
        bids: vec![["49999.00".to_owned(), "1.0".to_owned()]],
        asks: vec![["50001.00".to_owned(), "2.0".to_owned()]],
    });
    let snapshot = BinanceOrderBookSnapshot {
        last_update_id: 159,
        bids: vec![["49998.00".to_owned(), "3.0".to_owned()]],
        asks: vec![["50002.00".to_owned(), "4.0".to_owned()]],
    };

    sync_depth_book_from_snapshot(&mut book, snapshot).unwrap();

    assert_eq!(book.last_update_id, Some(160));
    assert_eq!(
        book.bids.get(&FixedDecimal::new(4_999_800, 2)),
        Some(&FixedDecimal::new(30, 1))
    );
    assert_eq!(
        book.bids.get(&FixedDecimal::new(4_999_900, 2)),
        Some(&FixedDecimal::new(10, 1))
    );
    assert_eq!(
        book.asks.get(&FixedDecimal::new(5_000_100, 2)),
        Some(&FixedDecimal::new(20, 1))
    );
    assert!(book.buffered_events.is_empty());
}

#[test]
fn diff_depth_gap_requires_snapshot_refetch() {
    let mut book = BinanceLocalOrderBook {
        last_update_id: Some(160),
        ..BinanceLocalOrderBook::default()
    };
    let event = BinanceDiffDepthMessage {
        symbol: "BTCUSDT".to_owned(),
        first_update_id: 163,
        final_update_id: 164,
        bids: vec![],
        asks: vec![],
    };

    let last_update_id = book.last_update_id.unwrap();
    assert!(event.first_update_id > last_update_id + 1);
    book.reset_for_resync(event);

    assert!(!book.is_synced());
    assert_eq!(book.buffered_events.len(), 1);
}

#[test]
fn rejects_partial_depth_without_combined_stream_symbol() {
    let error = normalize_binance_partial_depth_message(
        &config(),
        r#"{
          "lastUpdateId": 160,
          "bids": [["49999.50", "1.5"]],
          "asks": [["50000.50", "2.0"]]
        }"#,
        1672515782256,
        47,
    )
    .unwrap_err();

    assert!(error.to_string().contains("combined stream wrapper"));
}

#[test]
fn marks_crossed_partial_depth_as_invalid() {
    let snapshot = normalize_binance_partial_depth_message(
        &config(),
        r#"{
          "stream": "btcusdt@depth5",
          "data": {
            "lastUpdateId": 160,
            "bids": [["50001.00", "1.5"]],
            "asks": [["50000.00", "2.0"]]
          }
        }"#,
        1672515782256,
        48,
    )
    .unwrap();

    assert_eq!(snapshot.quality, EventQuality::Invalid);
}

#[test]
fn marks_delayed_ticker_without_throwing_away_payload() {
    let snapshot = normalize_binance_ticker_message(
        &config(),
        r#"{
          "e": "24hrTicker",
          "E": 1672515782136,
          "s": "BTCUSDT",
          "c": "50000.00",
          "b": "49999.50",
          "B": "1.5",
          "a": "50000.50",
          "A": "2.0",
          "L": 18150
        }"#,
        1672515784137,
        44,
    )
    .unwrap();

    assert_eq!(snapshot.quality, EventQuality::Delayed);
}

#[test]
fn tolerates_small_exchange_clock_skew() {
    let snapshot = normalize_binance_ticker_message(
        &config(),
        r#"{
          "e": "24hrTicker",
          "E": 1672515782136,
          "s": "BTCUSDT",
          "c": "50000.00",
          "b": "49999.50",
          "B": "1.5",
          "a": "50000.50",
          "A": "2.0",
          "L": 18150
        }"#,
        1672515782079,
        44,
    )
    .unwrap();

    assert_eq!(snapshot.quality, EventQuality::Ok);
    assert_eq!(snapshot.received_time_ms, snapshot.event_time_ms);
}

#[test]
fn rejects_unknown_symbol() {
    let error = normalize_binance_ticker_message(
        &config(),
        r#"{
          "e": "24hrTicker",
          "E": 1672515782136,
          "s": "ETHUSDT",
          "c": "3000.00",
          "b": "2999.50",
          "B": "1.5",
          "a": "3000.50",
          "A": "2.0",
          "L": 18150
        }"#,
        1672515782256,
        45,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unknown symbol"));
}
