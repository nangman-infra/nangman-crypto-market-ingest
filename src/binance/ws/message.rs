use super::super::events::{BinanceParsedEnvelope, parse_binance_payload};
use super::super::{BinanceIngestError, BinanceMarket, stats::BinanceL0WatchStats};
use crate::clock;
use crate::storage::L0StorageSink;
use crate::storage::record::RawMarketEventDraft;
use std::collections::HashMap;
use std::str;
use tokio_tungstenite::tungstenite;

pub(super) async fn handle_message(
    message: tungstenite::Message,
    stats: &mut BinanceL0WatchStats,
    storage: Option<&mut L0StorageSink>,
    markets_by_raw: &HashMap<String, BinanceMarket>,
) -> Result<(), BinanceIngestError> {
    match message {
        tungstenite::Message::Text(text) => {
            record_text_payload(text.as_ref(), stats, storage, markets_by_raw).await
        }
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref())?;
            record_text_payload(text, stats, storage, markets_by_raw).await
        }
        tungstenite::Message::Ping(_payload) => {
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
            stats.control_messages += 1;
            stats.close_messages += 1;
            stats.source_health_status = "closed".to_owned();
            stats.source_health_events += 1;
            Ok(())
        }
        tungstenite::Message::Frame(_) => Ok(()),
    }
}

async fn record_text_payload(
    raw_json: &str,
    stats: &mut BinanceL0WatchStats,
    storage: Option<&mut L0StorageSink>,
    markets_by_raw: &HashMap<String, BinanceMarket>,
) -> Result<(), BinanceIngestError> {
    stats.received_messages += 1;
    let detected_at_ms = clock::now_ms();
    match parse_binance_payload(raw_json) {
        Ok(envelope) => {
            if let Some(sink) = storage
                && let Some(draft) =
                    raw_market_event_draft(&envelope, markets_by_raw, detected_at_ms)
            {
                sink.append_raw_market_event(draft)
                    .await
                    .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
            }
            stats.record_event(envelope, detected_at_ms);
        }
        Err(error) => {
            stats.malformed_messages += 1;
            let _ = error;
        }
    }
    Ok(())
}

fn raw_market_event_draft(
    envelope: &BinanceParsedEnvelope,
    markets_by_raw: &HashMap<String, BinanceMarket>,
    ingest_timestamp_ms: i64,
) -> Option<RawMarketEventDraft> {
    let market = markets_by_raw.get(&envelope.event.symbol().to_ascii_uppercase())?;
    Some(RawMarketEventDraft {
        event_type: envelope.event.storage_event_type().to_owned(),
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.raw_symbol.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: envelope.event.exchange_timestamp_ms(ingest_timestamp_ms),
        ingest_timestamp_ms,
        sequence_id: envelope.event.sequence_id(),
        sequence_tag: envelope.event.sequence_id(),
        exchange_sequence: envelope.event.exchange_sequence(),
        diff_first_update_id: envelope.event.diff_first_update_id(),
        diff_final_update_id: envelope.event.diff_final_update_id(),
        is_snapshot: envelope.event.storage_event_type() == "depth_snapshot",
        stream_type: "REALTIME".to_owned(),
        stream_phase: "realtime".to_owned(),
        payload_json: envelope.payload_json.clone(),
    })
}
