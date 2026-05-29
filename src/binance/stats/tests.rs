use super::*;
use crate::binance::events::{BinanceDiffDepthMessage, BinanceParsedEnvelope, BinanceParsedEvent};
use crate::clock;

#[test]
fn records_depth_update_id_gap_alert() {
    let mut stats = BinanceL0WatchStats::new("wss://example.test".to_owned(), 1);
    stats.record_event(depth_envelope(41, 42), clock::now_ms());
    stats.record_event(depth_envelope(44, 45), clock::now_ms());

    assert_eq!(stats.gap_alert_count, 1);
    assert_eq!(stats.gap_alerts[0].gap_type, "depth_update_id_gap");
    assert_eq!(stats.gap_alerts[0].expected_sequence_id, Some(43));
    assert_eq!(stats.gap_alerts[0].observed_sequence_id, Some(44));
}

fn depth_envelope(first: i64, final_id: i64) -> BinanceParsedEnvelope {
    BinanceParsedEnvelope {
        stream: "btcusdt@depth@100ms".to_owned(),
        event: BinanceParsedEvent::DiffDepth(BinanceDiffDepthMessage {
            event_time_ms: 1,
            symbol: "BTCUSDT".to_owned(),
            first_update_id: first,
            final_update_id: final_id,
        }),
        payload_json: "{}".to_owned(),
    }
}
