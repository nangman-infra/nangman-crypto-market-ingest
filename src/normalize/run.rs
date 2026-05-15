use super::args::{InputRange, NormalizeArgs, unix_timestamp_millis};
use super::discovery::{resolve_last_l1_success_end_ms, resolve_oldest_l0_object_ms};
use super::mode::{RunDecision, decide_live_priority_mode, decide_mode};
use super::publish::publish_outputs;
use super::read::{cleanup_session_tmp, read_and_build_slices};
use super::write::index_pointer_key;
use crate::log_stream;
use crate::shutdown::ShutdownListener;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

const PREFLIGHT_ENV: &str = "MARKET_NORMALIZE_PREFLIGHT";
const MAX_AUDIT_WINDOWS: usize = 100_000;
const MAX_AUDIT_MISSING_SAMPLES: usize = 20;

pub async fn run_normalize(args: NormalizeArgs, now_ms: i64) -> Result<(), Box<dyn Error>> {
    if args.preflight || env_flag(PREFLIGHT_ENV) {
        run_preflight(&args, now_ms).await?;
        return Ok(());
    }

    if let (Some(start_ms), Some(end_ms)) =
        (args.audit_l1_index_start_ms, args.audit_l1_index_end_ms)
    {
        run_l1_index_audit(&args, InputRange { start_ms, end_ms }).await?;
        return Ok(());
    }

    if args.input_start_ms.is_some() {
        return run_backfill_once(&args, now_ms).await;
    }

    run_normalize_worker(args, now_ms).await
}

async fn run_backfill_once(args: &NormalizeArgs, now_ms: i64) -> Result<(), Box<dyn Error>> {
    let Some(decision) = decide_mode(args, now_ms, None, None)? else {
        return Ok(());
    };
    run_decision(args, decision, now_ms).await
}

async fn drain_ready_windows(
    args: &NormalizeArgs,
    initial_now_ms: i64,
    shutdown: Option<&ShutdownHandle>,
) -> Result<(), Box<dyn Error>> {
    let (mut last_l1_success_end_ms, mut oldest_l0_object_ms) =
        resolve_initial_l0_l1_state(args).await?;
    let mut processed_windows = 0_usize;
    let mut now_ms = initial_now_ms;
    let mut live_priority_completed_range = None;
    if let Some(completed_range) =
        run_live_priority_decision(args, now_ms, &mut last_l1_success_end_ms).await?
    {
        processed_windows += 1;
        live_priority_completed_range = Some(completed_range);
        oldest_l0_object_ms = None;
        now_ms = unix_timestamp_millis();
    }

    loop {
        if shutdown.is_some_and(ShutdownHandle::is_requested) {
            log_stream::info(
                "market_normalize_shutdown_complete",
                json!({
                    "processed_windows": processed_windows,
                    "last_l1_success_end_ms": last_l1_success_end_ms
                }),
            )?;
            return Ok(());
        }

        if processed_windows >= args.max_windows_per_tick {
            log_stream::warn(
                "market_normalize_max_windows_reached",
                json!({
                    "processed_windows": processed_windows,
                    "max_windows_per_tick": args.max_windows_per_tick,
                    "last_l1_success_end_ms": last_l1_success_end_ms
                }),
            )?;
            return Ok(());
        }

        let Some(completed_end_ms) = run_next_ready_decision(
            args,
            now_ms,
            last_l1_success_end_ms,
            oldest_l0_object_ms,
            live_priority_completed_range,
            processed_windows,
        )
        .await?
        else {
            return Ok(());
        };
        processed_windows += 1;
        last_l1_success_end_ms = Some(completed_end_ms);
        oldest_l0_object_ms = None;
        if shutdown.is_some_and(ShutdownHandle::is_requested) {
            log_stream::info(
                "market_normalize_shutdown_after_window",
                json!({
                    "processed_windows": processed_windows,
                    "last_l1_success_end_ms": last_l1_success_end_ms
                }),
            )?;
            return Ok(());
        }
        now_ms = unix_timestamp_millis();
    }
}

async fn resolve_initial_l0_l1_state(
    args: &NormalizeArgs,
) -> Result<(Option<i64>, Option<i64>), Box<dyn Error>> {
    let last_l1_success_end_ms = resolve_l1_success_end_ms_or_warn(args).await?;
    let oldest_l0_object_ms = if last_l1_success_end_ms.is_none() {
        resolve_oldest_l0_object_ms_or_warn(args).await?
    } else {
        None
    };
    Ok((last_l1_success_end_ms, oldest_l0_object_ms))
}

async fn resolve_l1_success_end_ms_or_warn(
    args: &NormalizeArgs,
) -> Result<Option<i64>, Box<dyn Error>> {
    match resolve_last_l1_success_end_ms(args).await {
        Ok(value) => Ok(value),
        Err(error) => {
            log_stream::warn(
                "market_normalize_l1_index_lookup_failed",
                json!({ "error": error.to_string() }),
            )?;
            Ok(None)
        }
    }
}

async fn resolve_oldest_l0_object_ms_or_warn(
    args: &NormalizeArgs,
) -> Result<Option<i64>, Box<dyn Error>> {
    match resolve_oldest_l0_object_ms(args).await {
        Ok(value) => Ok(value),
        Err(error) => {
            log_stream::warn(
                "market_normalize_l0_oldest_lookup_failed",
                json!({ "error": error.to_string() }),
            )?;
            Ok(None)
        }
    }
}

async fn run_live_priority_decision(
    args: &NormalizeArgs,
    now_ms: i64,
    last_l1_success_end_ms: &mut Option<i64>,
) -> Result<Option<InputRange>, Box<dyn Error>> {
    let Some(decision) = decide_live_priority_mode(args, now_ms, *last_l1_success_end_ms)? else {
        return Ok(None);
    };
    log_stream::info(
        "market_normalize_live_priority_selected",
        json!({
            "input_time_range_start_ms": decision.input_range.start_ms,
            "input_time_range_end_ms": decision.input_range.end_ms,
            "sequential_last_l1_success_end_ms": last_l1_success_end_ms,
            "live_priority_lag_threshold_ms": args.live_priority_lag_threshold_ms
        }),
    )?;
    let completed_range = decision.input_range;
    run_decision(args, decision, now_ms).await?;
    if *last_l1_success_end_ms == Some(completed_range.start_ms) {
        *last_l1_success_end_ms = Some(completed_range.end_ms);
    }
    Ok(Some(completed_range))
}

async fn run_next_ready_decision(
    args: &NormalizeArgs,
    now_ms: i64,
    last_l1_success_end_ms: Option<i64>,
    oldest_l0_object_ms: Option<i64>,
    live_priority_completed_range: Option<InputRange>,
    processed_windows: usize,
) -> Result<Option<i64>, Box<dyn Error>> {
    let Some(decision) = decide_mode(args, now_ms, last_l1_success_end_ms, oldest_l0_object_ms)?
    else {
        log_not_ready(
            now_ms,
            processed_windows,
            last_l1_success_end_ms,
            oldest_l0_object_ms,
        )?;
        return Ok(None);
    };

    let completed_end_ms = decision.input_range.end_ms;
    if live_priority_completed_range == Some(decision.input_range) {
        log_live_priority_duplicate_skipped(decision.input_range)?;
        return Ok(Some(completed_end_ms));
    }
    run_decision(args, decision, now_ms).await?;
    Ok(Some(completed_end_ms))
}

fn log_not_ready(
    now_ms: i64,
    processed_windows: usize,
    last_l1_success_end_ms: Option<i64>,
    oldest_l0_object_ms: Option<i64>,
) -> Result<(), Box<dyn Error>> {
    log_stream::debug(
        "market_normalize_not_ready",
        json!({
            "now_ms": now_ms,
            "processed_windows": processed_windows,
            "last_l1_success_end_ms": last_l1_success_end_ms,
            "oldest_l0_object_ms": oldest_l0_object_ms
        }),
    )?;
    Ok(())
}

fn log_live_priority_duplicate_skipped(input_range: InputRange) -> Result<(), Box<dyn Error>> {
    log_stream::info(
        "market_normalize_live_priority_duplicate_skipped",
        json!({
            "input_time_range_start_ms": input_range.start_ms,
            "input_time_range_end_ms": input_range.end_ms
        }),
    )?;
    Ok(())
}

async fn run_normalize_worker(
    args: NormalizeArgs,
    initial_now_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let mut shutdown = ShutdownHandle::install()?;
    let sleep_duration = Duration::from_millis(u64::try_from(args.schedule_interval_ms)?);
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
            return Ok(());
        }
        log_stream::debug(
            "market_normalize_worker_sleep",
            json!({ "sleep_ms": args.schedule_interval_ms }),
        )?;
        if shutdown.sleep_or_requested(sleep_duration).await {
            log_stream::info(
                "market_normalize_worker_stopped",
                json!({ "reason": "signal" }),
            )?;
            return Ok(());
        }
        now_ms = unix_timestamp_millis();
    }
}

struct ShutdownHandle {
    requested: Arc<AtomicBool>,
    waiter: Option<JoinHandle<()>>,
}

impl ShutdownHandle {
    fn install() -> Result<Self, Box<dyn Error>> {
        let requested = Arc::new(AtomicBool::new(false));
        let requested_for_task = Arc::clone(&requested);
        let mut listener = ShutdownListener::new()?;
        let waiter = tokio::spawn(async move {
            listener.wait().await;
            requested_for_task.store(true, Ordering::SeqCst);
        });
        Ok(Self {
            requested,
            waiter: Some(waiter),
        })
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    async fn sleep_or_requested(&mut self, duration: Duration) -> bool {
        if self.is_requested() {
            return true;
        }

        let Some(waiter) = self.waiter.as_mut() else {
            tokio::time::sleep(duration).await;
            return self.is_requested();
        };

        tokio::select! {
            biased;
            _ = waiter => {
                self.requested.store(true, Ordering::SeqCst);
                self.waiter = None;
                true
            }
            _ = tokio::time::sleep(duration) => self.is_requested(),
        }
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if let Some(waiter) = &self.waiter {
            waiter.abort();
        }
    }
}

async fn run_preflight(args: &NormalizeArgs, now_ms: i64) -> Result<(), Box<dyn Error>> {
    preflight_bucket(args, "l0", &args.l0_s3_bucket, now_ms).await?;
    preflight_bucket(args, "l1", &args.l1_s3_bucket, now_ms).await?;
    log_stream::info(
        "market_normalize_preflight_ok",
        json!({
            "aws_profile": args.aws_profile.as_deref(),
            "aws_region": args.aws_region.as_str(),
            "l0_s3_bucket": args.l0_s3_bucket.as_str(),
            "l1_s3_bucket": args.l1_s3_bucket.as_str()
        }),
    )?;
    Ok(())
}

async fn preflight_bucket(
    args: &NormalizeArgs,
    label: &str,
    bucket: &str,
    now_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let uploader = S3Uploader::new(
        bucket.to_owned(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;
    let prefix = "_preflight/market-ingest-app/";
    uploader.list_keys(prefix).await?;
    let key = format!("{prefix}{label}-{now_ms}-{}.json", std::process::id());
    let bytes = serde_json::to_vec(&json!({
        "schema_version": "market_normalize_s3_preflight_v1",
        "label": label,
        "bucket": bucket,
        "timestamp_ms": now_ms
    }))?;
    uploader.upload_json(&key, bytes).await?;

    let tmp_path = args
        .catchup_tmp_root
        .join("_preflight")
        .join(format!("{label}-{now_ms}.json"));
    let download_result = uploader.download_file(&key, &tmp_path).await;
    let delete_result = uploader.delete_object(&key).await;
    let _ = std::fs::remove_file(&tmp_path);
    download_result?;
    delete_result?;
    Ok(())
}

async fn run_l1_index_audit(args: &NormalizeArgs, range: InputRange) -> Result<(), Box<dyn Error>> {
    if range.end_ms <= range.start_ms {
        return Err("audit range must be positive and non-empty".into());
    }
    if range.start_ms % args.window_ms != 0 || range.end_ms % args.window_ms != 0 {
        return Err("audit range must align to window_ms".into());
    }
    let expected_keys = audit_expected_keys(range, args.window_ms)?;
    let expected_count = expected_keys.len();
    let uploader = S3Uploader::new(
        args.l1_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;

    let mut existing_keys = BTreeSet::new();
    for prefix in audit_hour_prefixes(&expected_keys) {
        for key in uploader.list_keys(&prefix).await? {
            existing_keys.insert(key);
        }
    }

    let mut missing = Vec::new();
    for key in &expected_keys {
        if !existing_keys.contains(key) {
            missing.push(key.clone());
        }
    }

    let missing_count = missing.len();
    let missing_samples = missing
        .iter()
        .take(MAX_AUDIT_MISSING_SAMPLES)
        .cloned()
        .collect::<Vec<_>>();
    log_stream::info(
        "market_normalize_l1_index_audit",
        json!({
            "l1_s3_bucket": args.l1_s3_bucket.as_str(),
            "window_ms": args.window_ms,
            "input_time_range_start_ms": range.start_ms,
            "input_time_range_end_ms": range.end_ms,
            "expected_index_pointer_count": expected_count,
            "missing_index_pointer_count": missing_count,
            "missing_index_pointer_samples": missing_samples
        }),
    )?;

    if missing_count == 0 {
        Ok(())
    } else {
        Err(
            format!("l1 index audit failed: missing {missing_count}/{expected_count} pointers")
                .into(),
        )
    }
}

fn audit_expected_keys(range: InputRange, window_ms: i64) -> Result<Vec<String>, Box<dyn Error>> {
    if window_ms <= 0 {
        return Err("window_ms must be positive".into());
    }
    let mut keys = Vec::new();
    let mut current = range.start_ms;
    while current < range.end_ms {
        if keys.len() >= MAX_AUDIT_WINDOWS {
            return Err(format!("audit range exceeds {MAX_AUDIT_WINDOWS} windows").into());
        }
        keys.push(index_pointer_key(window_ms, current));
        let Some(next) = current.checked_add(window_ms) else {
            return Err("audit range overflow".into());
        };
        if next <= current {
            return Err("audit range did not advance".into());
        }
        current = next;
    }
    Ok(keys)
}

fn audit_hour_prefixes(keys: &[String]) -> Vec<String> {
    let mut prefixes = BTreeMap::<String, ()>::new();
    for key in keys {
        if let Some((prefix, _)) = key.rsplit_once("window_start_ms=") {
            prefixes.insert(prefix.to_owned(), ());
        }
    }
    prefixes.into_keys().collect()
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

async fn run_decision(
    args: &NormalizeArgs,
    decision: RunDecision,
    now_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let started_at_ms = now_ms;
    let input_range = decision.input_range;
    let l1_run_id = format!(
        "l1_{}_{}_{}",
        input_range.start_ms, input_range.end_ms, started_at_ms
    );
    let result = run_decision_body(args, decision, &l1_run_id, started_at_ms, now_ms).await;
    // Enforce catchup_tmp_lifecycle invariant: clear the per-run tmp regardless of outcome.
    cleanup_session_tmp(&args.catchup_tmp_root, &l1_run_id).await;
    result
}

async fn run_decision_body(
    args: &NormalizeArgs,
    decision: RunDecision,
    l1_run_id: &str,
    started_at_ms: i64,
    now_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let input_range = decision.input_range;
    let scan_range = InputRange {
        start_ms: input_range.start_ms.saturating_sub(args.scan_margin_ms),
        end_ms: input_range.end_ms.saturating_add(args.scan_margin_ms),
    };
    let read_range = InputRange {
        start_ms: input_range
            .start_ms
            .saturating_sub(args.scan_margin_ms.max(args.projection_lookback_ms)),
        end_ms: scan_range.end_ms,
    };

    log_stream::debug(
        "market_normalize_started",
        json!({
            "l1_run_id": l1_run_id,
            "run_mode": decision.run_mode.as_str(),
            "input_start_ms": input_range.start_ms,
            "input_end_ms": input_range.end_ms,
            "scan_start_ms": scan_range.start_ms,
            "scan_end_ms": scan_range.end_ms,
            "read_start_ms": read_range.start_ms,
            "read_end_ms": read_range.end_ms,
            "projection_lookback_ms": args.projection_lookback_ms
        }),
    )?;

    let build = read_and_build_slices(
        args,
        input_range,
        scan_range,
        read_range,
        decision.run_mode,
        l1_run_id,
    )
    .await?;
    if build.fallback_alert {
        log_stream::warn(
            "market_normalize_fallback_alert",
            json!({
                "l1_run_id": l1_run_id,
                "run_mode": decision.run_mode.as_str(),
                "input_start_ms": input_range.start_ms,
                "input_end_ms": input_range.end_ms,
                "scan_start_ms": scan_range.start_ms,
                "scan_end_ms": scan_range.end_ms,
                "read_start_ms": read_range.start_ms,
                "read_end_ms": read_range.end_ms,
                "input_s3_object_count": build.input_s3_object_count,
                "input_local_object_count": build.input_local_object_count
            }),
        )?;
    }
    log_stream::debug(
        "market_normalize_building",
        json!({
            "phase": "finished",
            "l1_run_id": l1_run_id,
            "input_record_count": build.input_record_count,
            "slice_count_total": build.slices.len(),
            "status": build.status
        }),
    )?;
    publish_outputs(args, l1_run_id, input_range, build, started_at_ms, now_ms).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_expected_keys_cover_every_window() {
        let keys = audit_expected_keys(
            InputRange {
                start_ms: 0,
                end_ms: 3_000,
            },
            1_000,
        )
        .unwrap();

        assert_eq!(keys.len(), 3);
        assert!(keys[0].ends_with("window_start_ms=0.json"));
        assert!(keys[1].ends_with("window_start_ms=1000.json"));
        assert!(keys[2].ends_with("window_start_ms=2000.json"));
    }

    #[test]
    fn audit_hour_prefixes_deduplicate_sorted_prefixes() {
        let keys = vec![
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=01/window_start_ms=3600000.json"
                .to_owned(),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=0.json"
                .to_owned(),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=1000.json"
                .to_owned(),
        ];

        let prefixes = audit_hour_prefixes(&keys);

        assert_eq!(
            prefixes,
            vec![
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=01/".to_owned(),
            ]
        );
    }
}
