use super::{BinanceIngestError, BinanceMarket};
use crate::storage::record::RawMarketEventDraft;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub async fn fetch_depth_snapshot_draft(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &BinanceMarket,
    limit: u16,
    ingest_timestamp_ms: i64,
) -> Result<RawMarketEventDraft, BinanceIngestError> {
    let limit_string = limit.to_string();
    let value = client
        .get(format!(
            "{}/api/v3/depth",
            rest_base_url.trim_end_matches('/')
        ))
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

#[derive(Debug)]
pub struct FundingRateSnapshotBatch {
    pub drafts: Vec<RawMarketEventDraft>,
    pub supported_symbols: BTreeSet<String>,
}

pub async fn fetch_funding_rate_snapshot_batch(
    client: &reqwest::Client,
    futures_rest_base_url: &str,
    markets: &[BinanceMarket],
    ingest_timestamp_ms: i64,
) -> Result<FundingRateSnapshotBatch, BinanceIngestError> {
    let value = client
        .get(format!(
            "{}/fapi/v1/premiumIndex",
            futures_rest_base_url.trim_end_matches('/')
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let records = match value {
        Value::Array(records) => records,
        Value::Object(record) => vec![Value::Object(record)],
        _ => {
            return Err(BinanceIngestError::InvalidMessage(
                "Binance premiumIndex returned non-object response".to_owned(),
            ));
        }
    };
    let markets_by_symbol = markets
        .iter()
        .map(|market| (market.raw_symbol.as_str(), market))
        .collect::<BTreeMap<_, _>>();
    let mut drafts = Vec::new();
    let mut supported_symbols = BTreeSet::new();
    for record in records {
        let Some(symbol) = record.get("symbol").and_then(Value::as_str) else {
            continue;
        };
        supported_symbols.insert(symbol.to_owned());
        let Some(market) = markets_by_symbol.get(symbol) else {
            continue;
        };
        let Some(funding_rate) = string_or_number(record.get("lastFundingRate")) else {
            continue;
        };
        let exchange_timestamp_ms = record
            .get("time")
            .and_then(Value::as_i64)
            .unwrap_or(ingest_timestamp_ms);
        let payload = serde_json::json!({
            "provider": "binance_usdm",
            "source_endpoint": "/fapi/v1/premiumIndex",
            "symbol": symbol,
            "funding_rate": funding_rate,
            "lastFundingRate": funding_rate,
            "unit": "ratio",
            "event_time_ms": exchange_timestamp_ms,
            "raw": record
        });
        let sequence_tag = format!("binance:funding_rate:{symbol}:{exchange_timestamp_ms}");
        drafts.push(derivative_snapshot_draft(
            "funding_rate_snapshot",
            market,
            exchange_timestamp_ms,
            ingest_timestamp_ms,
            sequence_tag,
            payload,
        )?);
    }
    Ok(FundingRateSnapshotBatch {
        drafts,
        supported_symbols,
    })
}

pub async fn fetch_open_interest_snapshot_draft(
    client: &reqwest::Client,
    futures_rest_base_url: &str,
    market: &BinanceMarket,
    ingest_timestamp_ms: i64,
) -> Result<RawMarketEventDraft, BinanceIngestError> {
    let value = client
        .get(format!(
            "{}/fapi/v1/openInterest",
            futures_rest_base_url.trim_end_matches('/')
        ))
        .query(&[("symbol", market.raw_symbol.as_str())])
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let open_interest = string_or_number(value.get("openInterest")).ok_or_else(|| {
        BinanceIngestError::InvalidMessage(format!(
            "Binance openInterest missing openInterest for {}",
            market.raw_symbol
        ))
    })?;
    let exchange_timestamp_ms = value
        .get("time")
        .and_then(Value::as_i64)
        .unwrap_or(ingest_timestamp_ms);
    let payload = serde_json::json!({
        "provider": "binance_usdm",
        "source_endpoint": "/fapi/v1/openInterest",
        "symbol": market.raw_symbol.as_str(),
        "open_interest": open_interest,
        "openInterest": open_interest,
        "unit": "contracts",
        "event_time_ms": exchange_timestamp_ms,
        "raw": value
    });
    let sequence_tag = format!(
        "binance:open_interest:{}:{exchange_timestamp_ms}",
        market.raw_symbol
    );
    derivative_snapshot_draft(
        "open_interest_snapshot",
        market,
        exchange_timestamp_ms,
        ingest_timestamp_ms,
        sequence_tag,
        payload,
    )
}

fn derivative_snapshot_draft(
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

fn string_or_number(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}
