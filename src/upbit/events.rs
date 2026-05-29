mod message;
mod parsed;
mod parser;

pub use message::{UpbitOrderbookMessage, UpbitTradeMessage};
#[cfg(test)]
pub use message::{UpbitOrderbookUnit, UpbitTickerMessage};
pub use parsed::{UpbitParsedEnvelope, UpbitParsedEvent};
pub use parser::parse_upbit_payload;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_status_error_and_unknown_payloads() {
        let envelopes = parse_upbit_payload(
            r#"[
                {"status":"UP"},
                {"error":{"name":"INVALID_AUTH","message":"bad token"}},
                {"type":"custom","code":"KRW-BTC"}
            ]"#,
        )
        .unwrap();

        assert_eq!(envelopes.len(), 3);
        assert!(matches!(envelopes[0].event, UpbitParsedEvent::Status(_)));
        assert!(matches!(envelopes[1].event, UpbitParsedEvent::Error { .. }));
        assert!(matches!(envelopes[2].event, UpbitParsedEvent::Unknown(_)));
        assert_eq!(envelopes[0].event.kind_name(), "status");
        assert_eq!(envelopes[1].event.symbol(), None);
    }

    #[test]
    fn parses_trade_and_orderbook_metadata() {
        let trade = parse_upbit_payload(
            r#"{
                "type":"trade",
                "code":"KRW-BTC",
                "timestamp":100,
                "trade_timestamp":90,
                "trade_price":100.5,
                "trade_volume":0.25,
                "ask_bid":"BID",
                "sequential_id":42,
                "stream_type":"REALTIME"
            }"#,
        )
        .unwrap()
        .remove(0);
        let orderbook = parse_upbit_payload(
            r#"{
                "type":"orderbook",
                "code":"KRW-BTC",
                "timestamp":120,
                "total_ask_size":1.0,
                "total_bid_size":2.0,
                "orderbook_units":[{"ask_price":101.0,"bid_price":100.0,"ask_size":1.5,"bid_size":2.5}],
                "stream_type":"SNAPSHOT"
            }"#,
        )
        .unwrap()
        .remove(0);

        assert_eq!(trade.event.kind_name(), "trade");
        assert_eq!(trade.event.symbol(), Some("KRW-BTC"));
        assert_eq!(trade.event.exchange_timestamp_ms(), Some(90));
        assert_eq!(orderbook.event.kind_name(), "orderbook");
        assert_eq!(orderbook.event.exchange_timestamp_ms(), Some(120));
    }
}
