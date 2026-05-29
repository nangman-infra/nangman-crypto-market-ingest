use super::super::super::{BinanceIngestError, BinanceMarket};
use crate::storage::record::RawMarketEventDraft;
use serde_json::Value;

pub(super) fn derivative_snapshot_draft(
    event_type: &str,
    market: &BinanceMarket,
    exchange_timestamp_ms: i64,
    ingest_timestamp_ms: i64,
    sequence_tag: String,
    payload: Value,
) -> Result<RawMarketEventDraft, BinanceIngestError> {
    Ok(RawMarketEventDraft {
        event_type: event_type.to_owned(),
        venue: "binance".to_owned(),
        source_role: "derivatives".to_owned(),
        market_type: "usdm_perpetual".to_owned(),
        symbol_native: market.raw_symbol.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms,
        ingest_timestamp_ms,
        sequence_id: sequence_tag.clone(),
        sequence_tag,
        exchange_sequence: None,
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot: true,
        stream_type: "REST_SNAPSHOT".to_owned(),
        stream_phase: "snapshot".to_owned(),
        payload_json: serde_json::to_string(&payload)?,
    })
}
