use super::{BinanceIngestError, BinanceMarket, rest};
use crate::log_stream;
use crate::storage::L0StorageSink;
use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, Serialize)]
pub struct BinanceDerivativeSnapshotReport {
    pub funding_rate_snapshot_records: u64,
    pub open_interest_snapshot_records: u64,
    pub unsupported_futures_symbol_count: u64,
    pub failure_count: u64,
}

impl BinanceDerivativeSnapshotReport {
    pub fn total_records(&self) -> u64 {
        self.funding_rate_snapshot_records
            .saturating_add(self.open_interest_snapshot_records)
    }
}

pub async fn append_derivative_snapshots(
    futures_rest_base_url: &str,
    markets: &[BinanceMarket],
    ingest_timestamp_ms: i64,
    sink: &mut L0StorageSink,
) -> Result<BinanceDerivativeSnapshotReport, BinanceIngestError> {
    let client = reqwest::Client::new();
    let mut report = BinanceDerivativeSnapshotReport::default();
    let mut supported_futures_symbols = None::<BTreeSet<String>>;

    match rest::fetch_funding_rate_snapshot_batch(
        &client,
        futures_rest_base_url,
        markets,
        ingest_timestamp_ms,
    )
    .await
    {
        Ok(batch) => {
            supported_futures_symbols = Some(batch.supported_symbols);
            for draft in batch.drafts {
                sink.append_raw_market_event(draft)
                    .await
                    .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
                report.funding_rate_snapshot_records += 1;
            }
        }
        Err(error) => {
            report.failure_count += 1;
            log_stream::warn(
                "market_ingest_binance_derivative_snapshot_failed",
                serde_json::json!({
                    "endpoint": "/fapi/v1/premiumIndex",
                    "error": error.to_string()
                }),
            )?;
        }
    }

    for market in markets {
        if !should_fetch_open_interest(&supported_futures_symbols, market.raw_symbol.as_str()) {
            report.unsupported_futures_symbol_count += 1;
            log_stream::debug(
                "market_ingest_binance_derivative_snapshot_skipped",
                serde_json::json!({
                    "endpoint": "/fapi/v1/openInterest",
                    "symbol": market.raw_symbol.as_str(),
                    "reason": "symbol_not_listed_on_binance_usdm_futures"
                }),
            )?;
            continue;
        }
        match rest::fetch_open_interest_snapshot_draft(
            &client,
            futures_rest_base_url,
            market,
            ingest_timestamp_ms,
        )
        .await
        {
            Ok(draft) => {
                sink.append_raw_market_event(draft)
                    .await
                    .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
                report.open_interest_snapshot_records += 1;
            }
            Err(error) => {
                report.failure_count += 1;
                log_stream::warn(
                    "market_ingest_binance_derivative_snapshot_failed",
                    serde_json::json!({
                        "endpoint": "/fapi/v1/openInterest",
                        "symbol": market.raw_symbol.as_str(),
                        "error": error.to_string()
                    }),
                )?;
            }
        }
    }

    Ok(report)
}

fn should_fetch_open_interest(
    supported_futures_symbols: &Option<BTreeSet<String>>,
    symbol: &str,
) -> bool {
    supported_futures_symbols
        .as_ref()
        .is_none_or(|symbols| symbols.contains(symbol))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetches_open_interest_when_support_list_is_unavailable() {
        assert!(should_fetch_open_interest(&None, "BTCUSDT"));
    }

    #[test]
    fn skips_open_interest_for_symbols_not_listed_on_usdm_futures() {
        let supported = Some(BTreeSet::from(["BTCUSDT".to_owned(), "ETHUSDT".to_owned()]));

        assert!(should_fetch_open_interest(&supported, "BTCUSDT"));
        assert!(!should_fetch_open_interest(&supported, "USD1USDT"));
    }
}
