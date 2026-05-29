use super::super::{BinanceIngestError, BinanceMarket};
use super::url::market_data_url;
use crate::storage::record::RawMarketEventDraft;
use serde_json::Value;

pub async fn fetch_depth_snapshot_draft(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &BinanceMarket,
    limit: u16,
    ingest_timestamp_ms: i64,
) -> Result<RawMarketEventDraft, BinanceIngestError> {
    let limit_string = limit.to_string();
    let value = client
        .get(market_data_url(rest_base_url, "/api/v3/depth")?)
        .query(&[
            ("symbol", market.raw_symbol.as_str()),
            ("limit", limit_string.as_str()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let last_update_id = value
        .get("lastUpdateId")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            BinanceIngestError::InvalidMessage(format!(
                "Binance depth snapshot missing lastUpdateId for {}",
                market.raw_symbol
            ))
        })?;
    let sequence_tag = format!("binance:depth_snapshot:{last_update_id}");

    Ok(RawMarketEventDraft {
        event_type: "depth_snapshot".to_owned(),
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.raw_symbol.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: ingest_timestamp_ms,
        ingest_timestamp_ms,
        sequence_id: sequence_tag.clone(),
        sequence_tag,
        exchange_sequence: Some(last_update_id),
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot: true,
        stream_type: "REST_SNAPSHOT".to_owned(),
        stream_phase: "snapshot".to_owned(),
        payload_json: serde_json::to_string(&value)?,
    })
}
