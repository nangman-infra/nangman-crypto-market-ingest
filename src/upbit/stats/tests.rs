use super::*;
use crate::upbit::events::{
    UpbitOrderbookMessage, UpbitOrderbookUnit, UpbitParsedEvent, UpbitTickerMessage,
    UpbitTradeMessage,
};

#[test]
fn records_trade_ordering_violation_as_gap_alert() {
    let mut stats = UpbitIngestWatchStats::new("wss://example.test".to_owned(), 3);
    stats.received_messages = 2;
    stats.record_event(trade("KRW-BTC", 100, 20), 120);
    stats.record_event(trade("KRW-BTC", 90, 19), 130);
    stats.update_health();

    assert_eq!(stats.trade_messages, 2);
    assert_eq!(stats.sequence_anomalies, 1);
    assert_eq!(stats.gap_alert_count, 1);
    assert_eq!(stats.source_health_status, "degraded");
    assert_eq!(
        stats.recent_gap_alerts[0].heal_action,
        "mark_incomplete_and_continue"
    );
}

#[test]
fn records_orderbook_best_quote_and_ticker_counts() {
    let mut stats = UpbitIngestWatchStats::new("wss://example.test".to_owned(), 3);
    stats.received_messages = 2;
    stats.record_event(
        UpbitParsedEvent::Ticker(UpbitTickerMessage {
            event_type: "ticker".to_owned(),
            code: "KRW-BTC".to_owned(),
            timestamp: 100,
            trade_timestamp: Some(90),
            trade_price: Some(100.0),
            acc_trade_price_24h: Some(1_000.0),
            acc_trade_volume_24h: Some(10.0),
            stream_type: Some("REALTIME".to_owned()),
        }),
        110,
    );
    stats.record_event(
        UpbitParsedEvent::Orderbook(UpbitOrderbookMessage {
            event_type: "orderbook".to_owned(),
            code: "KRW-BTC".to_owned(),
            timestamp: 120,
            total_ask_size: 1.0,
            total_bid_size: 1.0,
            orderbook_units: vec![UpbitOrderbookUnit {
                ask_price: 101.0,
                bid_price: 100.0,
                ask_size: 1.5,
                bid_size: 2.5,
            }],
            stream_type: Some("SNAPSHOT".to_owned()),
            level: Some(5.0),
        }),
        130,
    );

    assert_eq!(stats.ticker_messages, 1);
    assert_eq!(stats.orderbook_messages, 1);
    assert_eq!(stats.derived_book_tickers, 1);
    assert_eq!(stats.kind_counts["ticker"], 1);
    assert_eq!(stats.symbol_counts["KRW-BTC"], 2);
    assert_eq!(stats.last_best_quotes["KRW-BTC"].best_bid, 100.0);
    assert_eq!(stats.heartbeat_delay_ms_at(150), 20);
}

#[test]
fn status_error_and_unknown_update_health_inputs() {
    let mut stats = UpbitIngestWatchStats::new("wss://example.test".to_owned(), 3);
    stats.record_event(UpbitParsedEvent::Status("UP".to_owned()), 100);
    stats.record_event(
        UpbitParsedEvent::Error {
            name: "BAD".to_owned(),
            message: "bad".to_owned(),
        },
        101,
    );
    stats.record_event(UpbitParsedEvent::Unknown(serde_json::json!({"x":1})), 102);
    stats.update_health();

    assert_eq!(stats.status_messages, 1);
    assert_eq!(stats.malformed_messages, 1);
    assert_eq!(stats.gap_alert_count, 1);
    assert_eq!(stats.source_health_status, "waiting_for_messages");
}

fn trade(code: &str, trade_timestamp: i64, sequential_id: i64) -> UpbitParsedEvent {
    UpbitParsedEvent::Trade(UpbitTradeMessage {
        event_type: "trade".to_owned(),
        code: code.to_owned(),
        timestamp: trade_timestamp,
        trade_timestamp,
        trade_price: 100.0,
        trade_volume: 0.1,
        ask_bid: "BID".to_owned(),
        sequential_id,
        best_ask_price: None,
        best_ask_size: None,
        best_bid_price: None,
        best_bid_size: None,
        stream_type: Some("REALTIME".to_owned()),
    })
}
