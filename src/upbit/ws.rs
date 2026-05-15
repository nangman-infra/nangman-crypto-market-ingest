use super::UpbitIngestError;
use super::events::{UpbitParsedEnvelope, UpbitParsedEvent, parse_upbit_payload};
use super::stats::UpbitIngestWatchStats;
use super::universe::UpbitMarket;
use crate::shutdown::ShutdownListener;
use crate::storage::L0StorageSink;
use crate::storage::record::RawMarketEventDraft;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::cmp;
use std::collections::HashMap;
use std::str;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::{connect_async, tungstenite};

pub async fn watch_upbit_ingest_streams(
    websocket_url: &str,
    markets: &[UpbitMarket],
    orderbook_unit: u8,
    duration: Duration,
    log_interval: Duration,
    mut storage: Option<&mut L0StorageSink>,
    log_callback: impl Fn(&UpbitIngestWatchStats),
) -> Result<UpbitIngestWatchStats, UpbitIngestError> {
    install_rustls_crypto_provider();

    let planned_stream_count = markets.len() * 3;
    let markets_by_code = markets
        .iter()
        .map(|market| (market.market.clone(), market.clone()))
        .collect::<HashMap<_, _>>();
    let mut stats = UpbitIngestWatchStats::new(websocket_url.to_owned(), planned_stream_count);
    let (mut websocket, _) = connect_async(websocket_url).await?;
    let mut shutdown = ShutdownListener::new().map_err(|error| {
        UpbitIngestError::InvalidMessage(format!("failed to install shutdown listener: {error}"))
    })?;
    websocket
        .send(tungstenite::Message::Text(
            subscription_message(markets, orderbook_unit).into(),
        ))
        .await?;

    let deadline = Instant::now() + duration;
    let mut next_log_at = Instant::now() + log_interval;
    let mut next_ping_at = Instant::now() + Duration::from_secs(30);

    while Instant::now() < deadline {
        let next_tick = cmp::min(cmp::min(deadline, next_log_at), next_ping_at);
        tokio::select! {
            _ = shutdown.wait() => {
                stats.source_health_status = "shutdown".to_owned();
                stats.source_health_events += 1;
                break;
            }
            message = timeout_at(next_tick, websocket.next()) => {
                match message {
                    Ok(Some(Ok(message))) => {
                        if let Err(error) = handle_message(
                            message,
                            &mut stats,
                            storage.as_deref_mut(),
                            &markets_by_code,
                        )
                        .await
                        {
                            stats.malformed_messages += 1;
                            stats.source_health_status = "message_error".to_owned();
                            stats.source_health_events += 1;
                            let _ = error;
                            break;
                        }
                    }
                    Ok(Some(Err(_error))) => {
                        stats.malformed_messages += 1;
                        stats.source_health_status = "websocket_error".to_owned();
                        stats.source_health_events += 1;
                        break;
                    }
                    Ok(None) => {
                        stats.source_health_status = "ended".to_owned();
                        stats.source_health_events += 1;
                        break;
                    }
                    Err(_) => {}
                }
            }
        }

        let now = Instant::now();
        if now >= next_ping_at {
            if websocket
                .send(tungstenite::Message::Ping(Vec::new().into()))
                .await
                .is_err()
            {
                stats.source_health_status = "ping_failed".to_owned();
                stats.source_health_events += 1;
                break;
            }
            next_ping_at += Duration::from_secs(30);
        }
        if now >= next_log_at {
            stats.update_health();
            log_callback(&stats);
            next_log_at += log_interval;
        }
    }

    stats.update_health();
    Ok(stats)
}

async fn handle_message(
    message: tungstenite::Message,
    stats: &mut UpbitIngestWatchStats,
    storage: Option<&mut L0StorageSink>,
    markets_by_code: &HashMap<String, UpbitMarket>,
) -> Result<(), UpbitIngestError> {
    match message {
        tungstenite::Message::Text(text) => {
            record_text_payload(text.as_ref(), stats, storage, markets_by_code).await
        }
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref())?;
            record_text_payload(text, stats, storage, markets_by_code).await
        }
        tungstenite::Message::Ping(_) => {
            stats.control_messages += 1;
            stats.pings_received += 1;
            Ok(())
        }
        tungstenite::Message::Pong(_) => {
            stats.control_messages += 1;
            stats.pongs_received += 1;
            Ok(())
        }
        tungstenite::Message::Close(_) => {
            stats.close_messages += 1;
            Ok(())
        }
        tungstenite::Message::Frame(_) => Ok(()),
    }
}

async fn record_text_payload(
    raw_json: &str,
    stats: &mut UpbitIngestWatchStats,
    mut storage: Option<&mut L0StorageSink>,
    markets_by_code: &HashMap<String, UpbitMarket>,
) -> Result<(), UpbitIngestError> {
    stats.received_messages += 1;
    let detected_at_ms = unix_timestamp_ms();
    match parse_upbit_payload(raw_json) {
        Ok(events) => {
            for envelope in events {
                if let Some(sink) = storage.as_deref_mut() {
                    for draft in raw_market_event_drafts(&envelope, markets_by_code, detected_at_ms)
                    {
                        sink.append_raw_market_event(draft)
                            .await
                            .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
                    }
                }
                stats.record_event(envelope.event, detected_at_ms);
            }
        }
        Err(error) => {
            stats.malformed_messages += 1;
            let _ = error;
        }
    }
    Ok(())
}

fn raw_market_event_drafts(
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

fn subscription_message(markets: &[UpbitMarket], orderbook_unit: u8) -> String {
    let codes = markets
        .iter()
        .map(|market| market.market.clone())
        .collect::<Vec<_>>();
    let orderbook_codes = markets
        .iter()
        .map(|market| format!("{}.{}", market.market, orderbook_unit))
        .collect::<Vec<_>>();
    json!([
        {"ticket": format!("nangman-market-ingest-upbit-{}", unix_timestamp_ms())},
        {"type": "ticker", "codes": codes},
        {"type": "trade", "codes": codes},
        {"type": "orderbook", "codes": orderbook_codes},
        {"format": "DEFAULT"}
    ])
    .to_string()
}

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
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
