mod args;
mod bootstrap;
mod worker_args;

pub use self::args::{SupervisorArgs, parse_args, print_help};
use self::bootstrap::{
    BootstrapChunk, BootstrapMarkerStore, next_missing_bootstrap_chunk, normalize_subchunks,
};
use self::worker_args::{backfill_args, normalize_args, normalize_backfill_args, realtime_args};

use crate::log_stream;
use crate::shutdown::ShutdownListener;
use crate::storage::{
    DualBucketRetention, S3RetentionLoopEvents, abort_s3_retention_handles,
    spawn_l0_l1_retention_loops,
};
use serde_json::json;
use std::error::Error;
use std::process::ExitStatus;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

type SchedulerError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn run_supervisor(args: SupervisorArgs) -> Result<(), Box<dyn Error>> {
    log_stream::info(
        "crypto_market_ingest_supervisor_start",
        json!({
            "realtime_venue": args.realtime_venue,
            "bootstrap_enabled": args.bootstrap_enabled,
            "bootstrap_lookback_days": args.bootstrap_lookback_days,
            "bootstrap_chunk_hours": args.bootstrap_chunk_hours,
            "l0_s3_bucket": args.l0_s3_bucket,
            "l1_s3_bucket": args.l1_s3_bucket,
            "l0_run_key_overlap_ms": args.l0_run_key_overlap_ms
        }),
    )?;

    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let mut shutdown_listener = ShutdownListener::new()?;
    let shutdown_for_task = Arc::clone(&shutdown_requested);
    let shutdown_task = tokio::spawn(async move {
        shutdown_listener.wait().await;
        shutdown_for_task.store(true, Ordering::SeqCst);
    });

    let mut realtime = spawn_realtime(&args)?;
    let mut normalize = if args.bootstrap_enabled {
        log_stream::info(
            "crypto_market_ingest_normalize_deferred",
            json!({ "reason": "bootstrap_l0_l1_in_progress" }),
        )?;
        None
    } else {
        Some(spawn_normalize(&args)?)
    };
    let mut backfill_scheduler = spawn_backfill_scheduler(args.clone(), &shutdown_requested);
    let mut retention_handles = Some(spawn_supervisor_s3_retention_loops(&args));
    let restart_delay = Duration::from_secs(args.restart_delay_secs);

    loop {
        tokio::select! {
            realtime_status = realtime.wait() => {
                let status = realtime_status?;
                shutdown_task.abort();
                shutdown_requested.store(true, Ordering::SeqCst);
                kill_optional_child(&mut normalize).await;
                backfill_scheduler.abort();
                abort_supervisor_retention(&mut retention_handles).await;
                return Err(format!("realtime worker exited: {status}").into());
            }
            normalize_status = wait_optional_child(&mut normalize) => {
                let status = normalize_status.expect("pending normalize wait cannot complete without a child")?;
                if shutdown_requested.load(Ordering::SeqCst) {
                    backfill_scheduler.abort();
                    abort_supervisor_retention(&mut retention_handles).await;
                    return Ok(());
                }
                log_stream::warn(
                    "crypto_market_ingest_normalize_restart",
                    json!({ "exit_status": status.to_string(), "restart_delay_secs": args.restart_delay_secs }),
                )?;
                tokio::time::sleep(restart_delay).await;
                normalize = Some(spawn_normalize(&args)?);
            }
            backfill_result = &mut backfill_scheduler => {
                if shutdown_requested.load(Ordering::SeqCst) {
                    kill_child(&mut realtime).await;
                    kill_optional_child(&mut normalize).await;
                    abort_supervisor_retention(&mut retention_handles).await;
                    return Ok(());
                }
                match backfill_result {
                    Ok(Ok(())) => {
                        log_stream::info(
                            "crypto_market_ingest_bootstrap_complete",
                            json!({ "next_worker": "normalize" }),
                        )?;
                        if normalize.is_none() {
                            normalize = Some(spawn_normalize(&args)?);
                        }
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
                kill_child(&mut realtime).await;
                kill_optional_child(&mut normalize).await;
                backfill_scheduler.abort();
                abort_supervisor_retention(&mut retention_handles).await;
                return Ok(());
            }
        }
    }
}

async fn wait_optional_child(child: &mut Option<Child>) -> Option<std::io::Result<ExitStatus>> {
    match child {
        Some(child) => Some(child.wait().await),
        None => std::future::pending().await,
    }
}

fn spawn_supervisor_s3_retention_loops(args: &SupervisorArgs) -> Vec<JoinHandle<()>> {
    spawn_l0_l1_retention_loops(DualBucketRetention {
        l0_bucket: args.l0_s3_bucket.clone(),
        l1_bucket: args.l1_s3_bucket.clone(),
        aws_region: args.aws_region.clone(),
        aws_profile: args.aws_profile.clone(),
        l0_retention_days: args.l0_s3_retention_days,
        l1_retention_days: args.l1_s3_retention_days,
        max_deletes_per_run: args.s3_retention_max_deletes_per_run,
        interval_secs: args.s3_retention_check_interval_secs,
        events: S3RetentionLoopEvents {
            run_event: "crypto_market_ingest_s3_retention_run",
            error_event: "crypto_market_ingest_s3_retention_error",
        },
    })
}

async fn abort_supervisor_retention(handles: &mut Option<Vec<JoinHandle<()>>>) {
    if let Some(handles) = handles.take() {
        abort_s3_retention_handles(handles).await;
    }
}

async fn shutdown_signal(task: &JoinHandle<()>) {
    while !task.is_finished() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn spawn_realtime(args: &SupervisorArgs) -> Result<Child, Box<dyn Error>> {
    let mut command = Command::new(&args.realtime_bin);
    command.args(realtime_args(args));
    spawn_child("realtime", command)
}

fn spawn_normalize(args: &SupervisorArgs) -> Result<Child, Box<dyn Error>> {
    let mut command = Command::new(&args.normalize_bin);
    command.args(normalize_args(args));
    spawn_child("normalize", command)
}

fn spawn_child(role: &str, mut command: Command) -> Result<Child, Box<dyn Error>> {
    log_stream::info(
        "crypto_market_ingest_worker_spawn",
        json!({ "worker_role": role }),
    )?;
    Ok(command.kill_on_drop(true).spawn()?)
}

fn spawn_backfill_scheduler(
    args: SupervisorArgs,
    shutdown_requested: &Arc<AtomicBool>,
) -> JoinHandle<Result<(), SchedulerError>> {
    let shutdown_requested = Arc::clone(shutdown_requested);
    tokio::spawn(async move { run_backfill_scheduler(args, shutdown_requested).await })
}

fn spawn_idle_backfill_scheduler(
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

async fn run_backfill_chunk(
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

async fn run_backfill_child(
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

async fn run_l1_normalize_chunk(
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

async fn wait_until_shutdown(shutdown_requested: &Arc<AtomicBool>) {
    while !shutdown_requested.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn sleep_or_shutdown(shutdown_requested: &Arc<AtomicBool>, seconds: u64) {
    for _ in 0..seconds {
        if shutdown_requested.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn kill_child(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    let _ = child.kill().await;
}

async fn kill_optional_child(child: &mut Option<Child>) {
    if let Some(child) = child {
        kill_child(child).await;
    }
}

#[cfg(test)]
mod tests {
    use super::bootstrap::bootstrap_chunks;
    use super::*;

    #[test]
    fn defaults_enable_all_in_one_contract() {
        let parsed = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        assert!(parsed.bootstrap_enabled);
        assert_eq!(parsed.bootstrap_lookback_days, 210);
        assert_eq!(parsed.realtime_venue, "binance");
        assert_eq!(parsed.l0_s3_bucket, args::DEFAULT_L0_S3_BUCKET);
        assert_eq!(parsed.l1_s3_bucket, args::DEFAULT_L1_S3_BUCKET);
        assert_eq!(
            parsed.l0_run_key_overlap_ms,
            args::DEFAULT_L0_RUN_KEY_OVERLAP_MS
        );
    }

    #[test]
    fn parses_bootstrap_symbols_and_knobs() {
        let parsed = parse_args(
            vec![
                "--bootstrap-symbols".to_owned(),
                "btcusdt, ethusdt".to_owned(),
                "--bootstrap-lookback-days".to_owned(),
                "2".to_owned(),
                "--bootstrap-chunk-hours".to_owned(),
                "6".to_owned(),
                "--l0-run-key-overlap-ms".to_owned(),
                "720000".to_owned(),
            ]
            .into_iter(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            parsed.bootstrap_symbols,
            Some(vec!["BTCUSDT".to_owned(), "ETHUSDT".to_owned()])
        );
        assert_eq!(parsed.bootstrap_lookback_days, 2);
        assert_eq!(parsed.bootstrap_chunk_hours, 6);
        assert_eq!(parsed.l0_run_key_overlap_ms, 720_000);
    }

    #[test]
    fn bootstrap_chunks_are_oldest_first_and_hour_aligned() {
        let mut args = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        args.bootstrap_lookback_days = 1;
        args.bootstrap_chunk_hours = 6;
        let now = 1778947200123;
        let chunks = bootstrap_chunks(&args, now);
        assert_eq!(chunks.len(), 4);
        assert!(
            chunks
                .windows(2)
                .all(|pair| pair[0].end_ms == pair[1].start_ms)
        );
        assert!(chunks.iter().all(|chunk| chunk.start_ms % 3_600_000 == 0));
        assert!(chunks.iter().all(|chunk| chunk.end_ms % 3_600_000 == 0));
        assert!(chunks.iter().all(|chunk| chunk.start_ms % 21_600_000 == 0));
        assert!(chunks.iter().all(|chunk| chunk.end_ms % 21_600_000 == 0));
    }

    #[test]
    fn bootstrap_chunks_do_not_shift_within_same_chunk_boundary() {
        let mut args = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        args.bootstrap_lookback_days = 2;
        args.bootstrap_chunk_hours = 24;
        let chunks_before = bootstrap_chunks(&args, 1778979600123);
        let chunks_after = bootstrap_chunks(&args, 1779004800123);

        assert_eq!(chunks_before, chunks_after);
        assert_eq!(chunks_before.len(), 2);
        assert!(
            chunks_before
                .windows(2)
                .all(|pair| pair[0].end_ms == pair[1].start_ms)
        );
        assert!(
            chunks_before
                .iter()
                .all(|chunk| chunk.start_ms % 86_400_000 == 0 && chunk.end_ms % 86_400_000 == 0)
        );
    }

    #[test]
    fn rejects_unstable_bootstrap_chunk_hours() {
        let err =
            parse_args(vec!["--bootstrap-chunk-hours".to_owned(), "7".to_owned()].into_iter())
                .unwrap_err()
                .to_string();

        assert!(err.contains("evenly divide 24"));
    }

    #[test]
    fn backfill_args_include_symbol_filter_when_configured() {
        let mut args = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        args.bootstrap_symbols = Some(vec!["BTCUSDT".to_owned()]);
        let values = backfill_args(
            &args,
            &BootstrapChunk {
                start_ms: 1,
                end_ms: 2,
            },
        );
        assert!(
            values
                .windows(2)
                .any(|pair| pair[0] == "--symbols" && pair[1] == "BTCUSDT")
        );
    }

    #[test]
    fn bootstrap_l1_normalize_subchunks_use_schedule_interval() {
        let mut args = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        args.normalize_schedule_interval_ms = 900_000;

        let chunks = normalize_subchunks(
            &args,
            BootstrapChunk {
                start_ms: 0,
                end_ms: 3_600_000,
            },
        );

        assert_eq!(chunks.len(), 4);
        assert_eq!(
            chunks.first(),
            Some(&BootstrapChunk {
                start_ms: 0,
                end_ms: 900_000
            })
        );
        assert_eq!(
            chunks.last(),
            Some(&BootstrapChunk {
                start_ms: 2_700_000,
                end_ms: 3_600_000
            })
        );
    }

    #[test]
    fn normalize_backfill_args_add_explicit_input_range() {
        let args = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        let values = normalize_backfill_args(
            &args,
            &BootstrapChunk {
                start_ms: 900_000,
                end_ms: 1_800_000,
            },
        );

        assert!(
            values
                .windows(2)
                .any(|pair| pair[0] == "--input-start-ms" && pair[1] == "900000")
        );
        assert!(
            values
                .windows(2)
                .any(|pair| pair[0] == "--input-end-ms" && pair[1] == "1800000")
        );
    }

    #[test]
    fn normalize_args_use_dedicated_run_key_overlap() {
        let mut args = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        args.realtime_duration_seconds = 31_536_000;
        args.l0_run_key_overlap_ms = 720_000;
        let values = normalize_args(&args);
        let overlap = values
            .windows(2)
            .find(|pair| pair[0] == "--l0-run-key-overlap-ms")
            .map(|pair| pair[1].clone());
        assert_eq!(overlap, Some("720000".to_owned()));
    }
}
