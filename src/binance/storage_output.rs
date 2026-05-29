mod finalize;
mod gaps;
mod health;
mod snapshots;

use super::stats::BinanceL0WatchStats;
use super::{BinanceIngestError, BinanceRunConfig};
use crate::storage::L0StorageSink;

pub(super) async fn append_depth_snapshots(
    config: &BinanceRunConfig,
    sink: &mut L0StorageSink,
) -> Result<u64, BinanceIngestError> {
    snapshots::append_depth_snapshots(config, sink).await
}

pub(super) async fn finalize_storage(
    sink: &mut L0StorageSink,
    stats: &BinanceL0WatchStats,
) -> Result<(), BinanceIngestError> {
    finalize::finalize_storage(sink, stats).await
}
