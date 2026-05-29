use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::log_stream;
use serde_json::json;

use super::child::{run_backfill_child, run_l1_normalize_chunk};
use crate::supervisor::bootstrap::{BootstrapChunk, BootstrapMarkerStore};
use crate::supervisor::{SchedulerError, SupervisorArgs};

pub(super) async fn run_backfill_chunk(
    args: &SupervisorArgs,
    marker_store: &BootstrapMarkerStore,
    chunk: BootstrapChunk,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<(), SchedulerError> {
    log_stream::info(
        "crypto_market_ingest_bootstrap_chunk_start",
        json!({
            "venue": "binance",
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "l0_marker_key": marker_store.l0_marker_key(&chunk),
            "complete_marker_key": marker_store.complete_marker_key(&chunk)
        }),
    )?;

    if marker_store.has_l0_success(&chunk).await? {
        log_stream::info(
            "crypto_market_ingest_bootstrap_l0_skip",
            json!({
                "input_start_ms": chunk.start_ms,
                "input_end_ms": chunk.end_ms,
                "reason": "l0_marker_exists"
            }),
        )?;
    } else if run_backfill_child(args, chunk, shutdown_requested).await? {
        marker_store.mark_l0_success(&chunk).await?;
    } else {
        return Ok(());
    }

    if !run_l1_normalize_chunk(args, marker_store, chunk, shutdown_requested).await? {
        return Ok(());
    }
    marker_store.mark_complete(&chunk).await?;
    log_stream::info(
        "crypto_market_ingest_bootstrap_chunk_done",
        json!({
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "complete_marker_key": marker_store.complete_marker_key(&chunk)
        }),
    )?;
    Ok(())
}
