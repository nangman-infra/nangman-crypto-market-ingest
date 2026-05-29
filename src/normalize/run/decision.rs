use super::{InputRange, NormalizeArgs, RunDecision};
use crate::log_stream;
use crate::normalize::publish::publish_outputs;
use crate::normalize::read::{cleanup_session_tmp, read_and_build_slices};
use serde_json::json;
use std::error::Error;

pub(super) async fn run_decision(
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
    let projection_lookback_ms = if args.live_priority_only {
        args.scan_margin_ms
    } else {
        args.projection_lookback_ms
    };
    let read_range = InputRange {
        start_ms: input_range
            .start_ms
            .saturating_sub(args.scan_margin_ms.max(projection_lookback_ms)),
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
            "projection_lookback_ms": projection_lookback_ms
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
