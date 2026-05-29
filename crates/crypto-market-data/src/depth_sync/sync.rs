mod alerts;
mod event;
mod snapshot;

use super::types::{BinanceDepthSyncSettings, BinanceLocalOrderBook};
use crate::error::MarketDataError;
use crate::messages::BinanceDiffDepthMessage;
use crate::stats::BinanceIngestWatchStats;
use crypto_domain::TimestampMs;
use std::collections::{BTreeMap, HashSet};

use event::{buffer_unsynced_depth_event, handle_synced_depth_event};
use snapshot::{fetch_and_sync_snapshot, should_fetch_snapshot};

pub(crate) async fn handle_diff_depth_event(
    depth_sync: &BinanceDepthSyncSettings,
    http_client: &reqwest::Client,
    event: BinanceDiffDepthMessage,
    received_time_ms: TimestampMs,
    books: &mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
) -> Result<(), MarketDataError> {
    let raw_symbol = event.symbol.to_ascii_uppercase();
    let book = books.entry(raw_symbol.clone()).or_default();
    if book.is_synced() {
        handle_synced_depth_event(
            book,
            event,
            &raw_symbol,
            received_time_ms,
            snapshot_attempted,
            stats,
        );
    } else {
        buffer_unsynced_depth_event(
            book,
            event,
            &raw_symbol,
            received_time_ms,
            snapshot_attempted,
            stats,
        );
    }

    if should_fetch_snapshot(books, snapshot_attempted, &raw_symbol) {
        fetch_and_sync_snapshot(
            depth_sync,
            http_client,
            books,
            snapshot_attempted,
            stats,
            &raw_symbol,
            received_time_ms,
        )
        .await?;
    }
    Ok(())
}
