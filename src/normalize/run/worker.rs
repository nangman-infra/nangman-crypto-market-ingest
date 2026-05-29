use crate::clock;
use crate::log_stream;
use crate::normalize::args::NormalizeArgs;
use drain::drain_ready_windows;
use retention::spawn_s3_retention_loops;
use schedule::worker_sleep_duration;
use serde_json::json;
use shutdown::ShutdownHandle;
use std::error::Error;

mod drain;
mod retention;
mod schedule;
mod shutdown;

pub(super) async fn run_normalize_worker(
    args: NormalizeArgs,
    initial_now_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let mut shutdown = ShutdownHandle::install()?;
    let mut retention_handles = Some(spawn_s3_retention_loops(&args));
    let sleep_duration = worker_sleep_duration(&args)?;
    let mut now_ms = initial_now_ms;
    log_stream::info(
        "market_normalize_worker_started",
        json!({
            "schedule_interval_ms": args.schedule_interval_ms,
            "max_windows_per_tick": args.max_windows_per_tick
        }),
    )?;

    loop {
        drain_ready_windows(&args, now_ms, Some(&shutdown)).await?;
        if shutdown.is_requested() {
            log_stream::info(
                "market_normalize_worker_stopped",
                json!({ "reason": "signal" }),
            )?;
            crate::storage::abort_s3_retention_handles(
                retention_handles.take().unwrap_or_default(),
            )
            .await;
            return Ok(());
        }
        log_stream::debug(
            "market_normalize_worker_sleep",
            json!({ "sleep_ms": sleep_duration.as_millis() }),
        )?;
        if shutdown.sleep_or_requested(sleep_duration).await {
            log_stream::info(
                "market_normalize_worker_stopped",
                json!({ "reason": "signal" }),
            )?;
            crate::storage::abort_s3_retention_handles(
                retention_handles.take().unwrap_or_default(),
            )
            .await;
            return Ok(());
        }
        now_ms = clock::now_ms();
    }
}
