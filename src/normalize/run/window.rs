use super::decision::run_decision;
use super::{InputRange, NormalizeArgs};
use crate::log_stream;
use crate::normalize::discovery::{resolve_last_l1_success_end_ms, resolve_oldest_l0_object_ms};
use crate::normalize::mode::{decide_live_priority_mode, decide_mode};
use serde_json::json;
use std::error::Error;

pub(super) async fn resolve_initial_l0_l1_state(
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

pub(super) async fn run_live_priority_decision(
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

pub(super) async fn run_next_ready_decision(
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
