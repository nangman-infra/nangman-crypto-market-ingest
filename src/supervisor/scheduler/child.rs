use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::log_stream;
use serde_json::json;
use tokio::process::Command;

use crate::supervisor::bootstrap::{BootstrapChunk, BootstrapMarkerStore, normalize_subchunks};
use crate::supervisor::process::kill_child;
use crate::supervisor::worker_args::{backfill_args, normalize_backfill_args};
use crate::supervisor::{SchedulerError, SupervisorArgs};

pub(super) async fn run_backfill_child(
    args: &SupervisorArgs,
    chunk: BootstrapChunk,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<bool, SchedulerError> {
    let mut command = Command::new(&args.backfill_bin);
    command.args(backfill_args(args, &chunk));
    let mut child = command.kill_on_drop(true).spawn()?;
    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            kill_child(&mut child).await;
            return Ok(false);
        }
        match child.try_wait()? {
            Some(status) if status.success() => {
                log_stream::info(
                    "crypto_market_ingest_bootstrap_l0_done",
                    json!({
                        "input_start_ms": chunk.start_ms,
                        "input_end_ms": chunk.end_ms,
                        "exit_status": status.to_string()
                    }),
                )?;
                return Ok(true);
            }
            Some(status) => {
                log_stream::warn(
                    "crypto_market_ingest_bootstrap_l0_failed",
                    json!({
                        "input_start_ms": chunk.start_ms,
                        "input_end_ms": chunk.end_ms,
                        "exit_status": status.to_string()
                    }),
                )?;
                return Ok(false);
            }
            None => tokio::time::sleep(Duration::from_secs(2)).await,
        }
    }
}

pub(super) async fn run_l1_normalize_chunk(
    args: &SupervisorArgs,
    marker_store: &BootstrapMarkerStore,
    chunk: BootstrapChunk,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<bool, SchedulerError> {
    for subchunk in normalize_subchunks(args, chunk) {
        if marker_store.has_l1_success(&subchunk).await? {
            log_stream::debug(
                "crypto_market_ingest_bootstrap_l1_skip",
                json!({
                    "input_start_ms": subchunk.start_ms,
                    "input_end_ms": subchunk.end_ms,
                    "reason": "l1_index_exists"
                }),
            )?;
            continue;
        }
        log_stream::info(
            "crypto_market_ingest_bootstrap_l1_start",
            json!({
                "input_start_ms": subchunk.start_ms,
                "input_end_ms": subchunk.end_ms
            }),
        )?;

        let mut command = Command::new(&args.normalize_bin);
        command.args(normalize_backfill_args(args, &subchunk));
        let mut child = command.kill_on_drop(true).spawn()?;
        loop {
            if shutdown_requested.load(Ordering::SeqCst) {
                kill_child(&mut child).await;
                return Ok(false);
            }
            match child.try_wait()? {
                Some(status) if status.success() => {
                    log_stream::info(
                        "crypto_market_ingest_bootstrap_l1_done",
                        json!({
                            "input_start_ms": subchunk.start_ms,
                            "input_end_ms": subchunk.end_ms,
                            "exit_status": status.to_string()
                        }),
                    )?;
                    break;
                }
                Some(status) => {
                    log_stream::warn(
                        "crypto_market_ingest_bootstrap_l1_failed",
                        json!({
                            "input_start_ms": subchunk.start_ms,
                            "input_end_ms": subchunk.end_ms,
                            "exit_status": status.to_string()
                        }),
                    )?;
                    return Ok(false);
                }
                None => tokio::time::sleep(Duration::from_secs(2)).await,
            }
        }
    }
    Ok(true)
}
