use super::super::super::{BinanceIngestError, BinanceMarket};
use super::super::payload::string_or_number;
use super::super::url::market_data_url;
use super::draft::derivative_snapshot_draft;
use crate::storage::record::RawMarketEventDraft;
use serde_json::Value;

const OPEN_INTEREST_ENDPOINT: &str = "/fapi/v1/openInterest";

pub async fn fetch_open_interest_snapshot_draft(
    client: &reqwest::Client,
    futures_rest_base_url: &str,
    market: &BinanceMarket,
    ingest_timestamp_ms: i64,
) -> Result<RawMarketEventDraft, BinanceIngestError> {
    let value = client
        .get(market_data_url(
            futures_rest_base_url,
            OPEN_INTEREST_ENDPOINT,
        )?)
        .query(&[("symbol", market.raw_symbol.as_str())])
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    open_interest_snapshot_draft_from_value(value, market, ingest_timestamp_ms)
}

fn open_interest_snapshot_draft_from_value(
    value: Value,
    market: &BinanceMarket,
    ingest_timestamp_ms: i64,
) -> Result<RawMarketEventDraft, BinanceIngestError> {
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
        "source_endpoint": OPEN_INTEREST_ENDPOINT,
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
