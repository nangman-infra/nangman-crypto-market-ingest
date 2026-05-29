use super::super::UpbitIngestError;
use super::super::events::parse_upbit_payload;
use super::super::stats::UpbitIngestWatchStats;
use super::super::universe::UpbitMarket;
use super::l0::raw_market_event_drafts;
use crate::clock;
use crate::storage::L0StorageSink;
use std::collections::HashMap;
use std::str;
use tokio_tungstenite::tungstenite;

pub(super) async fn handle_message(
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
    let detected_at_ms = clock::now_ms();
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
