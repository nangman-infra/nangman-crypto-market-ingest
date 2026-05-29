use super::super::events::{UpbitParsedEnvelope, UpbitParsedEvent};
use super::super::universe::UpbitMarket;
use crate::storage::record::RawMarketEventDraft;
use std::collections::HashMap;

pub(super) fn raw_market_event_drafts(
    envelope: &UpbitParsedEnvelope,
    markets_by_code: &HashMap<String, UpbitMarket>,
    ingest_timestamp_ms: i64,
) -> Vec<RawMarketEventDraft> {
    match &envelope.event {
        UpbitParsedEvent::Ticker(message) => markets_by_code
            .get(&message.code)
            .map(|market| {
                vec![draft(DraftInput {
                    event_type: "ticker",
                    market,
                    exchange_timestamp_ms: message.timestamp,
                    sequence_id: format!("upbit:ticker:ts-{}", message.timestamp),
                    stream_type: stream_type(message.stream_type.as_deref()),
                    exchange_sequence: None,
                    payload_json: &envelope.payload_json,
                    ingest_timestamp_ms,
                })]
            })
            .unwrap_or_default(),
        UpbitParsedEvent::Trade(message) => markets_by_code
            .get(&message.code)
            .map(|market| {
                vec![draft(DraftInput {
                    event_type: "trade",
                    market,
                    exchange_timestamp_ms: message.trade_timestamp,
                    sequence_id: format!("upbit:trade:{}", message.sequential_id),
                    stream_type: stream_type(message.stream_type.as_deref()),
                    exchange_sequence: Some(message.sequential_id),
                    payload_json: &envelope.payload_json,
                    ingest_timestamp_ms,
                })]
            })
            .unwrap_or_default(),
        UpbitParsedEvent::Orderbook(message) => markets_by_code
            .get(&message.code)
            .map(|market| {
                let stream_type = stream_type(message.stream_type.as_deref());
                vec![draft(DraftInput {
                    event_type: "depth_snapshot",
                    market,
                    exchange_timestamp_ms: message.timestamp,
                    sequence_id: format!("upbit:orderbook:ts-{}", message.timestamp),
                    stream_type,
                    exchange_sequence: None,
                    payload_json: &envelope.payload_json,
                    ingest_timestamp_ms,
                })]
            })
            .unwrap_or_default(),
        UpbitParsedEvent::Status(_)
        | UpbitParsedEvent::Error { .. }
        | UpbitParsedEvent::Unknown(_) => Vec::new(),
    }
}

struct DraftInput<'a> {
    event_type: &'a str,
    market: &'a UpbitMarket,
    exchange_timestamp_ms: i64,
    sequence_id: String,
    stream_type: String,
    exchange_sequence: Option<i64>,
    payload_json: &'a str,
    ingest_timestamp_ms: i64,
}

fn draft(input: DraftInput<'_>) -> RawMarketEventDraft {
    let is_snapshot = input.event_type == "depth_snapshot";
    RawMarketEventDraft {
        event_type: input.event_type.to_owned(),
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: input.market.market.clone(),
        symbol_canonical: input.market.base_asset.clone(),
        base_asset: input.market.base_asset.clone(),
        quote_asset: input.market.quote_asset.clone(),
        exchange_timestamp_ms: input.exchange_timestamp_ms,
        ingest_timestamp_ms: input.ingest_timestamp_ms,
        sequence_id: input.sequence_id.clone(),
        sequence_tag: input.sequence_id,
        exchange_sequence: input.exchange_sequence,
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot,
        stream_phase: stream_phase(&input.stream_type),
        stream_type: input.stream_type,
        payload_json: input.payload_json.to_owned(),
    }
}

fn stream_type(value: Option<&str>) -> String {
    value.unwrap_or("UNKNOWN").to_owned()
}

fn stream_phase(stream_type: &str) -> String {
    match stream_type {
        "SNAPSHOT" => "snapshot".to_owned(),
        "REALTIME" => "realtime".to_owned(),
        _ => "unknown".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upbit::events::{UpbitOrderbookMessage, UpbitOrderbookUnit};

    #[test]
    fn orderbook_l0_draft_is_single_depth_snapshot() {
        let market = UpbitMarket {
            market: "KRW-BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "KRW".to_owned(),
            korean_name: "Bitcoin".to_owned(),
            english_name: "Bitcoin".to_owned(),
            acc_trade_price_24h: 1.0,
        };
        let envelope = UpbitParsedEnvelope {
            event: UpbitParsedEvent::Orderbook(UpbitOrderbookMessage {
                event_type: "orderbook".to_owned(),
                code: "KRW-BTC".to_owned(),
                timestamp: 2,
                total_ask_size: 1.0,
                total_bid_size: 1.0,
                orderbook_units: vec![UpbitOrderbookUnit {
                    ask_price: 101.0,
                    bid_price: 100.0,
                    ask_size: 1.0,
                    bid_size: 1.0,
                }],
                stream_type: Some("SNAPSHOT".to_owned()),
                level: Some(5.0),
            }),
            payload_json: "{}".to_owned(),
        };
        let mut markets = HashMap::new();
        markets.insert("KRW-BTC".to_owned(), market);

        let drafts = raw_market_event_drafts(&envelope, &markets, 3);

        assert_eq!(drafts.len(), 1);
        assert_eq!(drafts[0].event_type, "depth_snapshot");
        assert!(drafts[0].is_snapshot);
        assert_eq!(drafts[0].stream_phase, "snapshot");
        assert!(drafts[0].exchange_sequence.is_none());
    }
}
