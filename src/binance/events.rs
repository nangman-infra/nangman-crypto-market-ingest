mod message;
mod parsed;
mod parser;

pub use message::{
    BinanceBookTickerMessage, BinanceDiffDepthMessage, BinanceTickerMessage, BinanceTradeMessage,
};
pub use parsed::{BinanceParsedEnvelope, BinanceParsedEvent};
pub use parser::parse_binance_payload;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_trade() {
        let envelope = parse_binance_payload(
            r#"{"stream":"btcusdt@trade","data":{"E":1,"s":"BTCUSDT","t":42,"T":2}}"#,
        )
        .unwrap();

        assert_eq!(envelope.event.storage_event_type(), "trade");
        assert_eq!(envelope.event.symbol(), "BTCUSDT");
        assert_eq!(envelope.event.sequence_id(), "binance:trade:42");
    }

    #[test]
    fn parses_combined_depth_delta() {
        let envelope = parse_binance_payload(
            r#"{"stream":"btcusdt@depth@100ms","data":{"E":1,"s":"BTCUSDT","U":41,"u":42}}"#,
        )
        .unwrap();

        assert_eq!(envelope.event.storage_event_type(), "depth_delta");
        assert_eq!(envelope.event.numeric_sequence(), 42);
        assert_eq!(envelope.event.diff_first_update_id(), Some(41));
        assert_eq!(envelope.event.diff_final_update_id(), Some(42));
    }
}
