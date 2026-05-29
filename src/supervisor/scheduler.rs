mod child;
mod chunk;
mod wait;

use super::bootstrap::{BootstrapMarkerStore, next_missing_bootstrap_chunk};
use super::{SchedulerError, SupervisorArgs};
use crate::log_stream;
use chunk::run_backfill_chunk;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;
use wait::{sleep_or_shutdown, wait_until_shutdown};

pub(super) fn spawn_backfill_scheduler(
    args: SupervisorArgs,
    shutdown_requested: &Arc<AtomicBool>,
) -> JoinHandle<Result<(), SchedulerError>> {
    let shutdown_requested = Arc::clone(shutdown_requested);
    tokio::spawn(async move { run_backfill_scheduler(args, shutdown_requested).await })
}

pub(super) fn spawn_idle_backfill_scheduler(
    shutdown_requested: &Arc<AtomicBool>,
) -> JoinHandle<Result<(), SchedulerError>> {
    let shutdown_requested = Arc::clone(shutdown_requested);
    tokio::spawn(async move {
        wait_until_shutdown(&shutdown_requested).await;
        Ok(())
    })
}

async fn run_backfill_scheduler(
    args: SupervisorArgs,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<(), SchedulerError> {
    if !args.bootstrap_enabled {
        log_stream::info(
            "crypto_market_ingest_bootstrap_disabled",
            json!({ "reason": "disabled_by_args" }),
        )?;
        wait_until_shutdown(&shutdown_requested).await;
        return Ok(());
    }

    let marker_store = BootstrapMarkerStore::new(&args).await?;
    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            return Ok(());
        }
        let Some(chunk) = next_missing_bootstrap_chunk(&args, &marker_store).await? else {
            log_stream::info(
                "crypto_market_ingest_bootstrap_complete",
                json!({
                    "lookback_days": args.bootstrap_lookback_days,
                    "chunk_hours": args.bootstrap_chunk_hours
                }),
            )?;
            return Ok(());
        };
        run_backfill_chunk(&args, &marker_store, chunk, &shutdown_requested).await?;
        sleep_or_shutdown(&shutdown_requested, args.bootstrap_interval_secs).await;
    }
}
