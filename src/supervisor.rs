use crate::log_stream;
use crate::normalize::write::index_pointer_key;
use crate::shutdown::ShutdownListener;
use crate::storage::{
    S3RetentionLoopEvents, abort_s3_retention_handles, l0_s3_retention_config,
    l1_s3_retention_config, s3_upload::S3Uploader, spawn_s3_retention_loop,
};
use serde_json::json;
use std::error::Error;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

const DEFAULT_CONFIG_DIR: &str = "/opt/nangman-crypto/strategies/crypto/rust-engine/config";
const DEFAULT_L0_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
const DEFAULT_L1_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l1";
const DEFAULT_CATCHUP_TMP_ROOT: &str = "/opt/nangman-crypto/data/spool/market-normalize/catchup";
const DEFAULT_REALTIME_BIN: &str = "/usr/local/bin/market-ingest-app";
const DEFAULT_BACKFILL_BIN: &str = "/usr/local/bin/market-backfill";
const DEFAULT_NORMALIZE_BIN: &str = "/usr/local/bin/market-normalize";
const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
const DEFAULT_L0_S3_BUCKET: &str = "nangman-crypto-dev-market-ingest-l0-962214";
const DEFAULT_L1_S3_BUCKET: &str = "nangman-crypto-dev-market-ingest-l1-962214";
const DEFAULT_RESTART_DELAY_SECS: u64 = 15;
const DEFAULT_BOOTSTRAP_LOOKBACK_DAYS: i64 = 210;
const DEFAULT_BOOTSTRAP_CHUNK_HOURS: i64 = 24;
const DEFAULT_BOOTSTRAP_INTERVAL_SECS: u64 = 60;
const DEFAULT_REALTIME_DURATION_SECONDS: u64 = 31_536_000;
const DEFAULT_L0_RUN_KEY_OVERLAP_MS: i64 = 360_000;
const DEFAULT_L0_S3_RETENTION_DAYS: i64 = 45;
const DEFAULT_L1_S3_RETENTION_DAYS: i64 = 240;
const DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS: u64 = 21_600;
const DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN: usize = 1_000;

#[derive(Debug, Clone)]
pub struct SupervisorArgs {
    pub config_dir: PathBuf,
    pub l0_s3_bucket: String,
    pub l1_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l1_spool_root: PathBuf,
    pub catchup_tmp_root: PathBuf,
    pub realtime_bin: PathBuf,
    pub backfill_bin: PathBuf,
    pub normalize_bin: PathBuf,
    pub realtime_venue: String,
    pub expect_symbol_count: usize,
    pub realtime_duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
    pub bootstrap_enabled: bool,
    pub bootstrap_lookback_days: i64,
    pub bootstrap_chunk_hours: i64,
    pub bootstrap_interval_secs: u64,
    pub bootstrap_symbols: Option<Vec<String>>,
    pub normalize_schedule_interval_ms: i64,
    pub l0_run_key_overlap_ms: i64,
    pub normalize_max_windows_per_tick: usize,
    pub l0_s3_retention_days: i64,
    pub l1_s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
    pub restart_delay_secs: u64,
}

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<SupervisorArgs>, Box<dyn Error>> {
    let mut parsed = SupervisorArgs {
        config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
        l0_s3_bucket: DEFAULT_L0_S3_BUCKET.to_owned(),
        l1_s3_bucket: DEFAULT_L1_S3_BUCKET.to_owned(),
        aws_profile: None,
        aws_region: DEFAULT_AWS_REGION.to_owned(),
        l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
        l1_spool_root: PathBuf::from(DEFAULT_L1_SPOOL_ROOT),
        catchup_tmp_root: PathBuf::from(DEFAULT_CATCHUP_TMP_ROOT),
        realtime_bin: PathBuf::from(DEFAULT_REALTIME_BIN),
        backfill_bin: PathBuf::from(DEFAULT_BACKFILL_BIN),
        normalize_bin: PathBuf::from(DEFAULT_NORMALIZE_BIN),
        realtime_venue: "binance".to_owned(),
        expect_symbol_count: 50,
        realtime_duration_seconds: DEFAULT_REALTIME_DURATION_SECONDS,
        log_interval_seconds: 30,
        l0_flush_records: 1_000,
        l0_shard_count: 1,
        bootstrap_enabled: true,
        bootstrap_lookback_days: DEFAULT_BOOTSTRAP_LOOKBACK_DAYS,
        bootstrap_chunk_hours: DEFAULT_BOOTSTRAP_CHUNK_HOURS,
        bootstrap_interval_secs: DEFAULT_BOOTSTRAP_INTERVAL_SECS,
        bootstrap_symbols: None,
        normalize_schedule_interval_ms: 900_000,
        l0_run_key_overlap_ms: DEFAULT_L0_RUN_KEY_OVERLAP_MS,
        normalize_max_windows_per_tick: 192,
        l0_s3_retention_days: DEFAULT_L0_S3_RETENTION_DAYS,
        l1_s3_retention_days: DEFAULT_L1_S3_RETENTION_DAYS,
        s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
        s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
        restart_delay_secs: DEFAULT_RESTART_DELAY_SECS,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--config" => parsed.config_dir = PathBuf::from(next_arg(&mut args, "--config")?),
            "--l0-s3-bucket" => parsed.l0_s3_bucket = next_arg(&mut args, "--l0-s3-bucket")?,
            "--l1-s3-bucket" => parsed.l1_s3_bucket = next_arg(&mut args, "--l1-s3-bucket")?,
            "--aws-profile" => parsed.aws_profile = Some(next_arg(&mut args, "--aws-profile")?),
            "--aws-region" => parsed.aws_region = next_arg(&mut args, "--aws-region")?,
            "--l0-spool-root" => {
                parsed.l0_spool_root = PathBuf::from(next_arg(&mut args, "--l0-spool-root")?);
            }
            "--l1-spool-root" => {
                parsed.l1_spool_root = PathBuf::from(next_arg(&mut args, "--l1-spool-root")?);
            }
            "--catchup-tmp-root" => {
                parsed.catchup_tmp_root = PathBuf::from(next_arg(&mut args, "--catchup-tmp-root")?);
            }
            "--realtime-bin" => {
                parsed.realtime_bin = PathBuf::from(next_arg(&mut args, "--realtime-bin")?);
            }
            "--backfill-bin" => {
                parsed.backfill_bin = PathBuf::from(next_arg(&mut args, "--backfill-bin")?);
            }
            "--normalize-bin" => {
                parsed.normalize_bin = PathBuf::from(next_arg(&mut args, "--normalize-bin")?);
            }
            "--realtime-venue" => {
                parsed.realtime_venue = next_arg(&mut args, "--realtime-venue")?;
            }
            "--expect-symbol-count" => {
                parsed.expect_symbol_count =
                    parse_positive_usize(next_arg(&mut args, "--expect-symbol-count")?)?;
            }
            "--realtime-duration-seconds" => {
                parsed.realtime_duration_seconds =
                    parse_positive_u64(next_arg(&mut args, "--realtime-duration-seconds")?)?;
            }
            "--log-interval-seconds" => {
                parsed.log_interval_seconds =
                    parse_positive_u64(next_arg(&mut args, "--log-interval-seconds")?)?;
            }
            "--l0-flush-records" => {
                parsed.l0_flush_records =
                    parse_positive_usize(next_arg(&mut args, "--l0-flush-records")?)?;
            }
            "--l0-shard-count" => {
                parsed.l0_shard_count =
                    parse_positive_u16(next_arg(&mut args, "--l0-shard-count")?)?;
            }
            "--disable-bootstrap" => parsed.bootstrap_enabled = false,
            "--bootstrap-lookback-days" => {
                parsed.bootstrap_lookback_days =
                    parse_positive_i64(next_arg(&mut args, "--bootstrap-lookback-days")?)?;
            }
            "--bootstrap-chunk-hours" => {
                parsed.bootstrap_chunk_hours =
                    parse_positive_i64(next_arg(&mut args, "--bootstrap-chunk-hours")?)?;
            }
            "--bootstrap-interval-secs" => {
                parsed.bootstrap_interval_secs =
                    parse_positive_u64(next_arg(&mut args, "--bootstrap-interval-secs")?)?;
            }
            "--bootstrap-symbols" => {
                parsed.bootstrap_symbols = Some(parse_symbols(
                    &next_arg(&mut args, "--bootstrap-symbols")?,
                    "--bootstrap-symbols",
                )?);
            }
            "--normalize-schedule-interval-ms" => {
                parsed.normalize_schedule_interval_ms =
                    parse_positive_i64(next_arg(&mut args, "--normalize-schedule-interval-ms")?)?;
            }
            "--l0-run-key-overlap-ms" => {
                parsed.l0_run_key_overlap_ms =
                    parse_positive_i64(next_arg(&mut args, "--l0-run-key-overlap-ms")?)?;
            }
            "--normalize-max-windows-per-tick" => {
                parsed.normalize_max_windows_per_tick =
                    parse_positive_usize(next_arg(&mut args, "--normalize-max-windows-per-tick")?)?;
            }
            "--l0-s3-retention-days" => {
                parsed.l0_s3_retention_days =
                    parse_positive_i64(next_arg(&mut args, "--l0-s3-retention-days")?)?;
            }
            "--l1-s3-retention-days" => {
                parsed.l1_s3_retention_days =
                    parse_positive_i64(next_arg(&mut args, "--l1-s3-retention-days")?)?;
            }
            "--s3-retention-check-interval-secs" => {
                parsed.s3_retention_check_interval_secs =
                    parse_positive_u64(next_arg(&mut args, "--s3-retention-check-interval-secs")?)?;
            }
            "--s3-retention-max-deletes-per-run" => {
                parsed.s3_retention_max_deletes_per_run = parse_positive_usize(next_arg(
                    &mut args,
                    "--s3-retention-max-deletes-per-run",
                )?)?;
            }
            "--restart-delay-secs" => {
                parsed.restart_delay_secs =
                    parse_positive_u64(next_arg(&mut args, "--restart-delay-secs")?)?;
            }
            _ => return Err(format!("unknown supervisor argument: {arg}").into()),
        }
    }

    if parsed.realtime_venue != "binance" && parsed.realtime_venue != "upbit" {
        return Err("--realtime-venue must be binance or upbit".into());
    }
    if parsed.bootstrap_lookback_days > 0 && parsed.bootstrap_chunk_hours > 24 {
        return Err("--bootstrap-chunk-hours must be <= 24 to keep recovery chunks bounded".into());
    }
    if 24 % parsed.bootstrap_chunk_hours != 0 {
        return Err(
            "--bootstrap-chunk-hours must evenly divide 24 for stable UTC day partitions".into(),
        );
    }
    Ok(Some(parsed))
}

pub fn print_help() {
    println!(
        r#"crypto-market-ingest-supervisor
Usage:
  crypto-market-ingest-supervisor \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214

Runs the all-in-one market data service:
  1. realtime L0 ingest
  2. historical bootstrap backfill
  3. long-lived L1 normalization

The ECS service should run this supervisor as the only container entrypoint."#
    );
}

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
    [
        (
            "l0",
            l0_s3_retention_config(
                args.l0_s3_bucket.clone(),
                args.aws_region.clone(),
                args.aws_profile.clone(),
                args.l0_s3_retention_days,
                args.s3_retention_max_deletes_per_run,
            ),
        ),
        (
            "l1",
            l1_s3_retention_config(
                args.l1_s3_bucket.clone(),
                args.aws_region.clone(),
                args.aws_profile.clone(),
                args.l1_s3_retention_days,
                args.s3_retention_max_deletes_per_run,
            ),
        ),
    ]
    .into_iter()
    .map(|(layer, config)| {
        spawn_s3_retention_loop(
            layer,
            config,
            args.s3_retention_check_interval_secs,
            S3RetentionLoopEvents {
                run_event: "crypto_market_ingest_s3_retention_run",
                error_event: "crypto_market_ingest_s3_retention_error",
            },
        )
    })
    .collect()
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
) -> JoinHandle<Result<(), String>> {
    let shutdown_requested = Arc::clone(shutdown_requested);
    tokio::spawn(async move { run_backfill_scheduler(args, shutdown_requested).await })
}

fn spawn_idle_backfill_scheduler(
    shutdown_requested: &Arc<AtomicBool>,
) -> JoinHandle<Result<(), String>> {
    let shutdown_requested = Arc::clone(shutdown_requested);
    tokio::spawn(async move {
        wait_until_shutdown(&shutdown_requested).await;
        Ok(())
    })
}

async fn run_backfill_scheduler(
    args: SupervisorArgs,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<(), String> {
    if !args.bootstrap_enabled {
        log_stream::info(
            "crypto_market_ingest_bootstrap_disabled",
            json!({ "reason": "disabled_by_args" }),
        )
        .map_err(|error| error.to_string())?;
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
            )
            .map_err(|error| error.to_string())?;
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
) -> Result<(), String> {
    log_stream::info(
        "crypto_market_ingest_bootstrap_chunk_start",
        json!({
            "venue": "binance",
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "l0_marker_key": marker_store.l0_marker_key(&chunk),
            "complete_marker_key": marker_store.complete_marker_key(&chunk)
        }),
    )
    .map_err(|error| error.to_string())?;

    if marker_store.has_l0_success(&chunk).await? {
        log_stream::info(
            "crypto_market_ingest_bootstrap_l0_skip",
            json!({
                "input_start_ms": chunk.start_ms,
                "input_end_ms": chunk.end_ms,
                "reason": "l0_marker_exists"
            }),
        )
        .map_err(|error| error.to_string())?;
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
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

async fn run_backfill_child(
    args: &SupervisorArgs,
    chunk: BootstrapChunk,
    shutdown_requested: &Arc<AtomicBool>,
) -> Result<bool, String> {
    let mut command = Command::new(&args.backfill_bin);
    command.args(backfill_args(args, &chunk));
    let mut child = command
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| error.to_string())?;
    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            kill_child(&mut child).await;
            return Ok(false);
        }
        match child.try_wait().map_err(|error| error.to_string())? {
            Some(status) if status.success() => {
                log_stream::info(
                    "crypto_market_ingest_bootstrap_l0_done",
                    json!({
                        "input_start_ms": chunk.start_ms,
                        "input_end_ms": chunk.end_ms,
                        "exit_status": status.to_string()
                    }),
                )
                .map_err(|error| error.to_string())?;
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
                )
                .map_err(|error| error.to_string())?;
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
) -> Result<bool, String> {
    for subchunk in normalize_subchunks(args, chunk) {
        if marker_store.has_l1_success(&subchunk).await? {
            log_stream::debug(
                "crypto_market_ingest_bootstrap_l1_skip",
                json!({
                    "input_start_ms": subchunk.start_ms,
                    "input_end_ms": subchunk.end_ms,
                    "reason": "l1_index_exists"
                }),
            )
            .map_err(|error| error.to_string())?;
            continue;
        }
        log_stream::info(
            "crypto_market_ingest_bootstrap_l1_start",
            json!({
                "input_start_ms": subchunk.start_ms,
                "input_end_ms": subchunk.end_ms
            }),
        )
        .map_err(|error| error.to_string())?;

        let mut command = Command::new(&args.normalize_bin);
        command.args(normalize_backfill_args(args, &subchunk));
        let mut child = command
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| error.to_string())?;
        loop {
            if shutdown_requested.load(Ordering::SeqCst) {
                kill_child(&mut child).await;
                return Ok(false);
            }
            match child.try_wait().map_err(|error| error.to_string())? {
                Some(status) if status.success() => {
                    log_stream::info(
                        "crypto_market_ingest_bootstrap_l1_done",
                        json!({
                            "input_start_ms": subchunk.start_ms,
                            "input_end_ms": subchunk.end_ms,
                            "exit_status": status.to_string()
                        }),
                    )
                    .map_err(|error| error.to_string())?;
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
                    )
                    .map_err(|error| error.to_string())?;
                    return Ok(false);
                }
                None => tokio::time::sleep(Duration::from_secs(2)).await,
            }
        }
    }
    Ok(true)
}

async fn next_missing_bootstrap_chunk(
    args: &SupervisorArgs,
    marker_store: &BootstrapMarkerStore,
) -> Result<Option<BootstrapChunk>, String> {
    for chunk in bootstrap_chunks(args, unix_timestamp_millis()) {
        if !marker_store.has_complete(&chunk).await? {
            return Ok(Some(chunk));
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BootstrapChunk {
    start_ms: i64,
    end_ms: i64,
}

struct BootstrapMarkerStore {
    uploader: S3Uploader,
}

impl BootstrapMarkerStore {
    async fn new(args: &SupervisorArgs) -> Result<Self, String> {
        let uploader = S3Uploader::new(
            args.l1_s3_bucket.clone(),
            args.aws_region.clone(),
            args.aws_profile.clone(),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(Self { uploader })
    }

    async fn has_complete(&self, chunk: &BootstrapChunk) -> Result<bool, String> {
        let marker = self
            .uploader
            .download_json_optional::<serde_json::Value>(&self.complete_marker_key(chunk))
            .await
            .map_err(|error| error.to_string())?;
        Ok(marker.is_some())
    }

    async fn has_l0_success(&self, chunk: &BootstrapChunk) -> Result<bool, String> {
        let marker = self
            .uploader
            .download_json_optional::<serde_json::Value>(&self.l0_marker_key(chunk))
            .await
            .map_err(|error| error.to_string())?;
        Ok(marker.is_some())
    }

    async fn has_l1_success(&self, chunk: &BootstrapChunk) -> Result<bool, String> {
        let pointer_key = index_pointer_key(1_000, chunk.start_ms);
        let Some(pointer) = self
            .uploader
            .download_json_optional::<serde_json::Value>(&pointer_key)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        Ok(pointer
            .get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status == "success")
            && pointer
                .get("input_time_range_start_ms")
                .and_then(serde_json::Value::as_i64)
                == Some(chunk.start_ms)
            && pointer
                .get("input_time_range_end_ms")
                .and_then(serde_json::Value::as_i64)
                == Some(chunk.end_ms))
    }

    async fn mark_l0_success(&self, chunk: &BootstrapChunk) -> Result<(), String> {
        let payload = serde_json::to_vec(&json!({
            "schema_version": "crypto_market_ingest_bootstrap_l0_marker_v1",
            "venue": "binance",
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "completed_at_ms": unix_timestamp_millis()
        }))
        .map_err(|error| error.to_string())?;
        self.uploader
            .upload_json(&self.l0_marker_key(chunk), payload)
            .await
            .map_err(|error| error.to_string())
    }

    async fn mark_complete(&self, chunk: &BootstrapChunk) -> Result<(), String> {
        let payload = serde_json::to_vec(&json!({
            "schema_version": "crypto_market_ingest_bootstrap_complete_marker_v1",
            "venue": "binance",
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "l0_marker_key": self.l0_marker_key(chunk),
            "completed_at_ms": unix_timestamp_millis()
        }))
        .map_err(|error| error.to_string())?;
        self.uploader
            .upload_json(&self.complete_marker_key(chunk), payload)
            .await
            .map_err(|error| error.to_string())
    }

    fn l0_marker_key(&self, chunk: &BootstrapChunk) -> String {
        format!(
            "supervisor/bootstrap/venue=binance/start_ms={}/end_ms={}/success.json",
            chunk.start_ms, chunk.end_ms
        )
    }

    fn complete_marker_key(&self, chunk: &BootstrapChunk) -> String {
        format!(
            "supervisor/bootstrap/venue=binance/start_ms={}/end_ms={}/complete.json",
            chunk.start_ms, chunk.end_ms
        )
    }
}

fn bootstrap_chunks(args: &SupervisorArgs, now_ms: i64) -> Vec<BootstrapChunk> {
    let lookback_ms = args
        .bootstrap_lookback_days
        .saturating_mul(24)
        .saturating_mul(3_600_000);
    let chunk_ms = args.bootstrap_chunk_hours.saturating_mul(3_600_000);
    if chunk_ms <= 0 {
        return Vec::new();
    }
    let end_bound = align_down_to_chunk(now_ms.saturating_sub(3_600_000), chunk_ms);
    let start_bound = align_down_to_chunk(end_bound.saturating_sub(lookback_ms), chunk_ms);
    let mut chunks = Vec::new();
    let mut cursor = start_bound;
    while cursor < end_bound {
        let end_ms = cursor.saturating_add(chunk_ms).min(end_bound);
        if end_ms > cursor {
            chunks.push(BootstrapChunk {
                start_ms: cursor,
                end_ms,
            });
        }
        cursor = end_ms;
    }
    chunks
}

fn normalize_subchunks(args: &SupervisorArgs, chunk: BootstrapChunk) -> Vec<BootstrapChunk> {
    let interval_ms = args.normalize_schedule_interval_ms;
    if interval_ms <= 0 || chunk.end_ms <= chunk.start_ms {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut cursor = chunk.start_ms;
    while cursor < chunk.end_ms {
        let end_ms = cursor.saturating_add(interval_ms).min(chunk.end_ms);
        if end_ms <= cursor {
            break;
        }
        chunks.push(BootstrapChunk {
            start_ms: cursor,
            end_ms,
        });
        cursor = end_ms;
    }
    chunks
}

fn realtime_args(args: &SupervisorArgs) -> Vec<String> {
    let mut values = vec![
        "--venue".to_owned(),
        args.realtime_venue.clone(),
        "--config".to_owned(),
        args.config_dir.display().to_string(),
        "--duration-seconds".to_owned(),
        args.realtime_duration_seconds.to_string(),
        "--log-interval-seconds".to_owned(),
        args.log_interval_seconds.to_string(),
        "--expect-symbol-count".to_owned(),
        args.expect_symbol_count.to_string(),
        "--allow-partial-symbol-coverage".to_owned(),
        "--l0-s3-bucket".to_owned(),
        args.l0_s3_bucket.clone(),
        "--aws-region".to_owned(),
        args.aws_region.clone(),
        "--l0-spool-root".to_owned(),
        args.l0_spool_root.display().to_string(),
        "--l0-flush-records".to_owned(),
        args.l0_flush_records.to_string(),
        "--l0-shard-count".to_owned(),
        args.l0_shard_count.to_string(),
        "--s3-retention-days".to_owned(),
        "45".to_owned(),
        "--disable-s3-retention".to_owned(),
    ];
    if args.realtime_venue == "binance" {
        values.extend([
            "--depth-snapshot-limit".to_owned(),
            "100".to_owned(),
            "--binance-futures-rest-base-url".to_owned(),
            "https://fapi.binance.com".to_owned(),
        ]);
    }
    if let Some(profile) = &args.aws_profile {
        values.extend(["--aws-profile".to_owned(), profile.clone()]);
    }
    values
}

fn backfill_args(args: &SupervisorArgs, chunk: &BootstrapChunk) -> Vec<String> {
    let mut values = vec![
        "--venue".to_owned(),
        "binance".to_owned(),
        "--config".to_owned(),
        args.config_dir.display().to_string(),
        "--input-start-ms".to_owned(),
        chunk.start_ms.to_string(),
        "--input-end-ms".to_owned(),
        chunk.end_ms.to_string(),
        "--expect-symbol-count".to_owned(),
        args.expect_symbol_count.to_string(),
        "--l0-s3-bucket".to_owned(),
        args.l0_s3_bucket.clone(),
        "--aws-region".to_owned(),
        args.aws_region.clone(),
        "--l0-spool-root".to_owned(),
        args.l0_spool_root.display().to_string(),
        "--l0-flush-records".to_owned(),
        args.l0_flush_records.to_string(),
        "--l0-shard-count".to_owned(),
        args.l0_shard_count.to_string(),
        "--s3-retention-days".to_owned(),
        "45".to_owned(),
        "--disable-s3-retention".to_owned(),
    ];
    if let Some(symbols) = &args.bootstrap_symbols {
        values.extend(["--symbols".to_owned(), symbols.join(",")]);
    }
    if let Some(profile) = &args.aws_profile {
        values.extend(["--aws-profile".to_owned(), profile.clone()]);
    }
    values
}

fn normalize_args(args: &SupervisorArgs) -> Vec<String> {
    let mut values = vec![
        "--l0-s3-bucket".to_owned(),
        args.l0_s3_bucket.clone(),
        "--l0-local-root".to_owned(),
        args.l0_spool_root.display().to_string(),
        "--l1-s3-bucket".to_owned(),
        args.l1_s3_bucket.clone(),
        "--aws-region".to_owned(),
        args.aws_region.clone(),
        "--schedule-interval-ms".to_owned(),
        args.normalize_schedule_interval_ms.to_string(),
        "--window-ms".to_owned(),
        "1000".to_owned(),
        "--scan-margin-ms".to_owned(),
        "300000".to_owned(),
        "--watermark-delay-ms".to_owned(),
        "360000".to_owned(),
        "--clock-skew-margin-ms".to_owned(),
        "1000".to_owned(),
        "--max-latency-ms".to_owned(),
        "1000".to_owned(),
        "--l0-run-key-overlap-ms".to_owned(),
        args.l0_run_key_overlap_ms.to_string(),
        "--max-windows-per-tick".to_owned(),
        args.normalize_max_windows_per_tick.to_string(),
        "--live-priority".to_owned(),
        "--live-priority-lag-threshold-ms".to_owned(),
        "900000".to_owned(),
        "--spool-root".to_owned(),
        args.l1_spool_root.display().to_string(),
        "--catchup-tmp-root".to_owned(),
        args.catchup_tmp_root.display().to_string(),
        "--l0-s3-retention-days".to_owned(),
        "45".to_owned(),
        "--l1-s3-retention-days".to_owned(),
        "240".to_owned(),
        "--disable-s3-retention".to_owned(),
    ];
    if let Some(profile) = &args.aws_profile {
        values.extend(["--aws-profile".to_owned(), profile.clone()]);
    }
    values
}

fn normalize_backfill_args(args: &SupervisorArgs, chunk: &BootstrapChunk) -> Vec<String> {
    let mut values = normalize_args(args);
    values.extend([
        "--input-start-ms".to_owned(),
        chunk.start_ms.to_string(),
        "--input-end-ms".to_owned(),
        chunk.end_ms.to_string(),
    ]);
    values
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

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn parse_positive_i64(value: String) -> Result<i64, Box<dyn Error>> {
    let parsed = value.parse::<i64>()?;
    if parsed <= 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: String) -> Result<u64, Box<dyn Error>> {
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_positive_usize(value: String) -> Result<usize, Box<dyn Error>> {
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_positive_u16(value: String) -> Result<u16, Box<dyn Error>> {
    let parsed = value.parse::<u16>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_symbols(value: &str, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let symbols = value
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(format!("{name} requires at least one symbol").into());
    }
    Ok(symbols)
}

fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn align_down_to_chunk(value: i64, chunk_ms: i64) -> i64 {
    value.div_euclid(chunk_ms) * chunk_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_enable_all_in_one_contract() {
        let parsed = parse_args(Vec::<String>::new().into_iter())
            .unwrap()
            .unwrap();
        assert!(parsed.bootstrap_enabled);
        assert_eq!(parsed.bootstrap_lookback_days, 210);
        assert_eq!(parsed.realtime_venue, "binance");
        assert_eq!(parsed.l0_s3_bucket, DEFAULT_L0_S3_BUCKET);
        assert_eq!(parsed.l1_s3_bucket, DEFAULT_L1_S3_BUCKET);
        assert_eq!(parsed.l0_run_key_overlap_ms, DEFAULT_L0_RUN_KEY_OVERLAP_MS);
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
