use super::super::super::{BinanceIngestError, BinanceMarket};
use super::super::payload::string_or_number;
use super::super::url::market_data_url;
use super::draft::derivative_snapshot_draft;
use super::types::FundingRateSnapshotBatch;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const PREMIUM_INDEX_ENDPOINT: &str = "/fapi/v1/premiumIndex";

pub async fn fetch_funding_rate_snapshot_batch(
    client: &reqwest::Client,
    futures_rest_base_url: &str,
    markets: &[BinanceMarket],
    ingest_timestamp_ms: i64,
) -> Result<FundingRateSnapshotBatch, BinanceIngestError> {
    let value = client
        .get(market_data_url(
            futures_rest_base_url,
            PREMIUM_INDEX_ENDPOINT,
        )?)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    funding_snapshot_batch_from_value(value, markets, ingest_timestamp_ms)
}

pub(super) fn funding_snapshot_batch_from_value(
    value: Value,
    markets: &[BinanceMarket],
    ingest_timestamp_ms: i64,
) -> Result<FundingRateSnapshotBatch, BinanceIngestError> {
    let records = premium_index_records(value)?;
    let markets_by_symbol = markets
        .iter()
        .map(|market| (market.raw_symbol.as_str(), market))
        .collect::<BTreeMap<_, _>>();
    let mut drafts = Vec::new();
    let mut supported_symbols = BTreeSet::new();
    for record in records {
        let Some(symbol) = record
            .get("symbol")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            continue;
        };
        supported_symbols.insert(symbol.clone());
        let Some(market) = markets_by_symbol.get(symbol.as_str()) else {
            continue;
        };
        let Some(funding_rate) = string_or_number(record.get("lastFundingRate")) else {
            continue;
        };
        drafts.push(funding_snapshot_draft(
            record,
            &symbol,
            market,
            funding_rate,
            ingest_timestamp_ms,
        )?);
    }
    Ok(FundingRateSnapshotBatch {
        drafts,
        supported_symbols,
    })
}

fn premium_index_records(value: Value) -> Result<Vec<Value>, BinanceIngestError> {
    match value {
        Value::Array(records) => Ok(records),
        Value::Object(record) => Ok(vec![Value::Object(record)]),
        _ => Err(BinanceIngestError::InvalidMessage(
            "Binance premiumIndex returned non-object response".to_owned(),
        )),
    }
}

fn funding_snapshot_draft(
    record: Value,
    symbol: &str,
    market: &BinanceMarket,
    funding_rate: String,
    ingest_timestamp_ms: i64,
) -> Result<crate::storage::record::RawMarketEventDraft, BinanceIngestError> {
    let exchange_timestamp_ms = record
        .get("time")
        .and_then(Value::as_i64)
        .unwrap_or(ingest_timestamp_ms);
    let payload = serde_json::json!({
        "provider": "binance_usdm",
        "source_endpoint": PREMIUM_INDEX_ENDPOINT,
        "symbol": symbol,
        "funding_rate": funding_rate,
        "lastFundingRate": funding_rate,
        "unit": "ratio",
        "event_time_ms": exchange_timestamp_ms,
        "raw": record
    });
    let sequence_tag = format!("binance:funding_rate:{symbol}:{exchange_timestamp_ms}");
    derivative_snapshot_draft(
        "funding_rate_snapshot",
        market,
        exchange_timestamp_ms,
        ingest_timestamp_ms,
        sequence_tag,
        payload,
    )
}
