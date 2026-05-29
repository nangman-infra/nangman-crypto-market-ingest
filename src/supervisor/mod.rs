mod args;
mod bootstrap;
mod process;
mod retention;
mod scheduler;
mod worker_args;

pub use self::args::{SupervisorArgs, parse_args, print_help};
#[cfg(test)]
use self::bootstrap::{BootstrapChunk, normalize_subchunks};
use self::process::{
    kill_optional_child, kill_realtime_children, shutdown_signal, spawn_live_priority_normalize,
    spawn_normalize, spawn_normalize_for_phase, spawn_realtime_children, wait_any_realtime_child,
    wait_optional_child,
};
use self::retention::{abort_supervisor_retention, spawn_supervisor_s3_retention_loops};
use self::scheduler::{spawn_backfill_scheduler, spawn_idle_backfill_scheduler};
#[cfg(test)]
use self::worker_args::{
    backfill_args, live_priority_normalize_args, normalize_args, normalize_backfill_args,
};

use crate::log_stream;
use crate::shutdown::ShutdownListener;
use serde_json::json;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

type SchedulerError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn run_supervisor(args: SupervisorArgs) -> Result<(), Box<dyn Error>> {
    log_stream::info(
        "crypto_market_ingest_supervisor_start",
        json!({
            "realtime_venue": &args.realtime_venue,
            "realtime_venues": &args.realtime_venues,
            "bootstrap_enabled": args.bootstrap_enabled,
            "bootstrap_lookback_days": args.bootstrap_lookback_days,
            "bootstrap_chunk_hours": args.bootstrap_chunk_hours,
            "l0_s3_bucket": &args.l0_s3_bucket,
            "l1_s3_bucket": &args.l1_s3_bucket,
            "l0_run_key_overlap_ms": args.l0_run_key_overlap_ms,
            "market_live_nats_enabled": args.live_nats_url.is_some(),
            "market_live_nats_stream": &args.live_nats_stream,
            "market_live_nats_subject_prefix": &args.live_nats_subject_prefix
        }),
    )?;

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let mut shutdown_listener = ShutdownListener::new()?;
    let shutdown_for_task = Arc::clone(&shutdown_requested);
    let shutdown_task = tokio::spawn(async move {
        shutdown_listener.wait().await;
        shutdown_for_task.store(true, Ordering::SeqCst);
    });

    let mut realtime = spawn_realtime_children(&args)?;
    let mut bootstrap_active = args.bootstrap_enabled;
    let mut normalize = if bootstrap_active {
        log_stream::info(
            "crypto_market_ingest_live_priority_normalize_start",
            json!({
                "reason": "bootstrap_l0_l1_in_progress",
                "max_windows_per_tick": 1
            }),
        )?;
        Some(spawn_live_priority_normalize(&args)?)
    } else {
        Some(spawn_normalize(&args)?)
    };
    let mut backfill_scheduler = spawn_backfill_scheduler(args.clone(), &shutdown_requested);
    let mut retention_handles = Some(spawn_supervisor_s3_retention_loops(&args));
    let restart_delay = Duration::from_secs(args.restart_delay_secs);

    loop {
        tokio::select! {
            realtime_status = wait_any_realtime_child(&mut realtime) => {
                let (venue, status) = realtime_status?;
                shutdown_task.abort();
                shutdown_requested.store(true, Ordering::SeqCst);
                kill_optional_child(&mut normalize).await;
                backfill_scheduler.abort();
                abort_supervisor_retention(&mut retention_handles).await;
                return Err(format!("realtime worker exited venue={venue}: {status}").into());
            }
            normalize_status = wait_optional_child(&mut normalize) => {
                let Some(status) = normalize_status else {
                    return Err("normalize wait completed without a child".into());
                };
                let status = status?;
                if shutdown_requested.load(Ordering::SeqCst) {
                    backfill_scheduler.abort();
                    abort_supervisor_retention(&mut retention_handles).await;
                    return Ok(());
                }
                log_stream::warn(
                    "crypto_market_ingest_normalize_restart",
                    json!({
                        "exit_status": status.to_string(),
                        "restart_delay_secs": args.restart_delay_secs,
                        "bootstrap_active": bootstrap_active
                    }),
                )?;
                tokio::time::sleep(restart_delay).await;
                normalize = Some(spawn_normalize_for_phase(&args, bootstrap_active)?);
            }
            backfill_result = &mut backfill_scheduler => {
                if shutdown_requested.load(Ordering::SeqCst) {
                    kill_realtime_children(&mut realtime).await;
                    kill_optional_child(&mut normalize).await;
                    abort_supervisor_retention(&mut retention_handles).await;
                    return Ok(());
                }
                match backfill_result {
                    Ok(Ok(())) => {
                        bootstrap_active = false;
                        log_stream::info(
                            "crypto_market_ingest_bootstrap_complete",
                            json!({ "next_worker": "full_normalize" }),
                        )?;
                        kill_optional_child(&mut normalize).await;
                        normalize = Some(spawn_normalize(&args)?);
                        backfill_scheduler = spawn_idle_backfill_scheduler(&shutdown_requested);
                        continue;
                    }
                    Ok(Err(error)) => {
                        log_stream::warn(
                            "crypto_market_ingest_backfill_scheduler_error",
                            json!({ "error": error.to_string(), "restart_delay_secs": args.restart_delay_secs }),
                        )?;
                    }
                    Err(error) => {
                        log_stream::warn(
                            "crypto_market_ingest_backfill_scheduler_join_error",
                            json!({ "error": error.to_string(), "restart_delay_secs": args.restart_delay_secs }),
                        )?;
                    }
                }
                tokio::time::sleep(restart_delay).await;
                backfill_scheduler = spawn_backfill_scheduler(args.clone(), &shutdown_requested);
            }
            _ = shutdown_signal(&shutdown_task) => {
                shutdown_requested.store(true, Ordering::SeqCst);
                log_stream::info("crypto_market_ingest_supervisor_shutdown", json!({}))?;
                kill_realtime_children(&mut realtime).await;
                kill_optional_child(&mut normalize).await;
                backfill_scheduler.abort();
                abort_supervisor_retention(&mut retention_handles).await;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests;
