use super::super::rest;
use super::super::{BinanceIngestError, BinanceRunConfig};
use crate::clock;
use crate::storage::L0StorageSink;

pub(super) async fn append_depth_snapshots(
    config: &BinanceRunConfig,
    sink: &mut L0StorageSink,
) -> Result<u64, BinanceIngestError> {
    let client = reqwest::Client::new();
    let mut appended = 0;
    for market in &config.markets {
        let draft = rest::fetch_depth_snapshot_draft(
            &client,
            &config.rest_base_url,
            market,
            config.depth_snapshot_limit,
            clock::now_ms(),
        )
        .await?;
        sink.append_raw_market_event(draft)
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
        appended += 1;
    }
    Ok(appended)
}
