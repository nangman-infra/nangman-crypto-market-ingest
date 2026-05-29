use super::super::args::{InputRange, NormalizeArgs};
use super::range::{align_floor, validate_range};
use super::types::{RunDecision, RunMode};
use std::error::Error;

/// Pick the latest closed watermark window before normal sequential catch-up.
///
/// This is deliberately separate from `decide_mode`: sequential catch-up keeps
/// the durable contiguous history correct, while live-priority keeps downstream
/// Intel workers from waiting hours for the newest Market-L1 index during a
/// backlog.
pub fn decide_live_priority_mode(
    args: &NormalizeArgs,
    now_ms: i64,
    last_l1_success_end_ms: Option<i64>,
) -> Result<Option<RunDecision>, Box<dyn Error>> {
    if !args.live_priority || args.input_start_ms.is_some() {
        return Ok(None);
    }

    let interval = args.schedule_interval_ms;
    let ready_end_ms = (now_ms / interval) * interval;
    if ready_end_ms <= 0 || now_ms < ready_end_ms.saturating_add(args.watermark_delay_ms) {
        return Ok(None);
    }

    if let Some(last_success_end_ms) = last_l1_success_end_ms {
        if last_success_end_ms >= ready_end_ms {
            return Ok(None);
        }

        let lag_ms = ready_end_ms.saturating_sub(last_success_end_ms);
        if lag_ms < args.live_priority_lag_threshold_ms {
            return Ok(None);
        }
    }

    let start_ms = ready_end_ms.saturating_sub(interval);
    let end_ms = ready_end_ms;
    validate_range(start_ms, end_ms, args.window_ms, args.schedule_interval_ms)?;
    Ok(Some(RunDecision {
        run_mode: RunMode::Live,
        input_range: InputRange { start_ms, end_ms },
    }))
}

/// Decide which window to process and what mode to use.
///
/// Returns `Ok(None)` when there is nothing to do yet (LIVE watermark not
/// reached, caught up, or no L0 data exists at all).
pub fn decide_mode(
    args: &NormalizeArgs,
    now_ms: i64,
    last_l1_success_end_ms: Option<i64>,
    oldest_l0_object_ms: Option<i64>,
) -> Result<Option<RunDecision>, Box<dyn Error>> {
    if let (Some(start_ms), Some(end_ms)) = (args.input_start_ms, args.input_end_ms) {
        validate_range(start_ms, end_ms, args.window_ms, args.schedule_interval_ms)?;
        return Ok(Some(RunDecision {
            run_mode: RunMode::Backfill,
            input_range: InputRange { start_ms, end_ms },
        }));
    }

    let interval = args.schedule_interval_ms;
    let ready_end_ms = (now_ms / interval) * interval;
    let next_start_candidate = match last_l1_success_end_ms {
        Some(value) => value,
        None => match oldest_l0_object_ms {
            Some(value) => align_floor(value, interval),
            None => return Ok(None),
        },
    };
    let next_start_ms = align_floor(next_start_candidate, interval);
    if next_start_ms.saturating_add(interval) > ready_end_ms {
        return Ok(None);
    }

    let start_ms = next_start_ms;
    let end_ms = next_start_ms.saturating_add(interval);
    validate_range(start_ms, end_ms, args.window_ms, args.schedule_interval_ms)?;
    let run_mode = if end_ms == ready_end_ms {
        RunMode::Live
    } else {
        RunMode::CatchUp
    };
    if run_mode == RunMode::Live && now_ms < ready_end_ms.saturating_add(args.watermark_delay_ms) {
        return Ok(None);
    }
    Ok(Some(RunDecision {
        run_mode,
        input_range: InputRange { start_ms, end_ms },
    }))
}
