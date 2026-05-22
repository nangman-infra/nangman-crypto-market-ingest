use super::args::{InputRange, NormalizeArgs};
use std::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Live,
    CatchUp,
    Backfill,
}

impl RunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RunMode::Live => "LIVE",
            RunMode::CatchUp => "CATCH-UP",
            RunMode::Backfill => "BACKFILL",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RunDecision {
    pub run_mode: RunMode,
    pub input_range: InputRange,
}

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

fn align_floor(value: i64, interval: i64) -> i64 {
    if interval <= 0 {
        return value;
    }
    let remainder = value.rem_euclid(interval);
    value.saturating_sub(remainder)
}

fn validate_range(
    start_ms: i64,
    end_ms: i64,
    window_ms: i64,
    schedule_interval_ms: i64,
) -> Result<(), Box<dyn Error>> {
    if start_ms < 0 || end_ms <= start_ms {
        return Err("input range must be positive and non-empty".into());
    }
    if window_ms > 0 && (start_ms % window_ms != 0 || end_ms % window_ms != 0) {
        return Err("input range must align to window_ms".into());
    }
    if schedule_interval_ms > 0
        && (start_ms % schedule_interval_ms != 0 || end_ms % schedule_interval_ms != 0)
    {
        return Err("input range must align to schedule_interval_ms".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn args() -> NormalizeArgs {
        NormalizeArgs {
            l0_s3_bucket: "l0".to_owned(),
            l0_local_root: PathBuf::from("/tmp/l0"),
            l1_s3_bucket: "l1".to_owned(),
            aws_profile: None,
            aws_region: "ap-northeast-2".to_owned(),
            input_start_ms: None,
            input_end_ms: None,
            schedule_interval_ms: 900_000, // 15 min
            window_ms: 1_000,
            scan_margin_ms: 300_000,
            projection_lookback_ms: 3_600_000,
            watermark_delay_ms: 360_000,
            clock_skew_margin_ms: 1_000,
            max_latency_ms: 1_000,
            l0_run_key_overlap_ms: 360_000,
            spool_root: PathBuf::from("/tmp/l1"),
            catchup_tmp_root: PathBuf::from("/tmp/catchup"),
            preflight: false,
            audit_l1_index_start_ms: None,
            audit_l1_index_end_ms: None,
            max_windows_per_tick: 192,
            live_priority: false,
            live_priority_lag_threshold_ms: 900_000,
            s3_retention_enabled: true,
            l0_s3_retention_days: 45,
            l1_s3_retention_days: 240,
            s3_retention_check_interval_secs: 21_600,
            s3_retention_max_deletes_per_run: 1_000,
        }
    }

    #[test]
    fn returns_none_before_watermark() {
        let now = 900_000 + 360_000 - 1; // just under watermark
        let decision = decide_mode(&args(), now, None, Some(0)).unwrap();
        assert!(decision.is_none());
    }

    #[test]
    fn returns_none_when_no_l0_and_no_l1() {
        let now = 1_800_000 + 360_000;
        let decision = decide_mode(&args(), now, None, None).unwrap();
        assert!(decision.is_none());
    }

    #[test]
    fn live_mode_when_gap_is_small() {
        let interval = 900_000_i64;
        let now = 100 * interval + 360_000;
        let last_success = 99 * interval; // 1 window behind, ~15 min gap
        let decision = decide_mode(&args(), now, Some(last_success), None)
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::Live);
        assert_eq!(decision.input_range.start_ms, 99 * interval);
        assert_eq!(decision.input_range.end_ms, 100 * interval);
    }

    #[test]
    fn catch_up_mode_when_backlog_is_older_than_latest_ready_window() {
        let interval = 900_000_i64;
        let now = 1_000 * interval + 360_000;
        let last_success = (1_000 - 28) * interval; // 28 windows = 7h
        let decision = decide_mode(&args(), now, Some(last_success), None)
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::CatchUp);
        assert_eq!(decision.input_range.start_ms, last_success);
        assert_eq!(decision.input_range.end_ms, last_success + interval);
    }

    #[test]
    fn catch_up_does_not_skip_small_backlog() {
        let interval = 900_000_i64;
        let now = 100 * interval + 360_000;
        let last_success = 96 * interval; // 4 windows behind
        let decision = decide_mode(&args(), now, Some(last_success), None)
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::CatchUp);
        assert_eq!(decision.input_range.start_ms, last_success);
        assert_eq!(decision.input_range.end_ms, last_success + interval);
    }

    #[test]
    fn catch_up_does_not_wait_for_latest_live_watermark() {
        let interval = 900_000_i64;
        let now = 100 * interval + 360_000 - 1;
        let last_success = 96 * interval; // next window is older than ready_end
        let decision = decide_mode(&args(), now, Some(last_success), None)
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::CatchUp);
        assert_eq!(decision.input_range.start_ms, last_success);
        assert_eq!(decision.input_range.end_ms, last_success + interval);
    }

    #[test]
    fn catch_up_picks_oldest_l0_when_no_l1_history() {
        let interval = 900_000_i64;
        let now = 1_000 * interval + 360_000;
        let oldest_l0 = (1_000 - 50) * interval + 100; // not aligned
        let decision = decide_mode(&args(), now, None, Some(oldest_l0))
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::CatchUp);
        assert_eq!(decision.input_range.start_ms, (1_000 - 50) * interval);
    }

    #[test]
    fn backfill_mode_when_explicit_range_provided() {
        let mut a = args();
        a.input_start_ms = Some(900_000);
        a.input_end_ms = Some(1_800_000);
        let decision = decide_mode(&a, 9_999_999_999, Some(0), Some(0))
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::Backfill);
        assert_eq!(decision.input_range.start_ms, 900_000);
        assert_eq!(decision.input_range.end_ms, 1_800_000);
    }

    #[test]
    fn returns_none_when_caught_up_and_next_window_not_ready() {
        let interval = 900_000_i64;
        let now = 100 * interval + 360_000;
        // last success already covers the latest closed window
        let last_success = 100 * interval;
        let decision = decide_mode(&args(), now, Some(last_success), None).unwrap();
        assert!(decision.is_none());
    }

    #[test]
    fn run_mode_string_representation_matches_contract() {
        assert_eq!(RunMode::Live.as_str(), "LIVE");
        assert_eq!(RunMode::CatchUp.as_str(), "CATCH-UP");
        assert_eq!(RunMode::Backfill.as_str(), "BACKFILL");
    }

    #[test]
    fn live_priority_picks_latest_closed_window_when_backlog_lags() {
        let interval = 900_000_i64;
        let mut a = args();
        a.live_priority = true;
        a.live_priority_lag_threshold_ms = interval;
        let now = 100 * interval + 360_000;
        let last_success = 90 * interval;
        let decision = decide_live_priority_mode(&a, now, Some(last_success))
            .unwrap()
            .unwrap();
        assert_eq!(decision.run_mode, RunMode::Live);
        assert_eq!(decision.input_range.start_ms, 99 * interval);
        assert_eq!(decision.input_range.end_ms, 100 * interval);
    }

    #[test]
    fn live_priority_can_seed_current_window_without_recent_l1_history() {
        let interval = 900_000_i64;
        let mut a = args();
        a.live_priority = true;
        let now = 100 * interval + 360_000;
        let decision = decide_live_priority_mode(&a, now, None).unwrap().unwrap();

        assert_eq!(decision.run_mode, RunMode::Live);
        assert_eq!(decision.input_range.start_ms, 99 * interval);
        assert_eq!(decision.input_range.end_ms, 100 * interval);
    }

    #[test]
    fn live_priority_stays_idle_when_latest_window_is_already_done() {
        let interval = 900_000_i64;
        let mut a = args();
        a.live_priority = true;
        let now = 100 * interval + 360_000;
        let decision = decide_live_priority_mode(&a, now, Some(100 * interval)).unwrap();
        assert!(decision.is_none());
    }

    #[test]
    fn live_priority_stays_idle_before_watermark() {
        let interval = 900_000_i64;
        let mut a = args();
        a.live_priority = true;
        let now = 100 * interval + 360_000 - 1;
        let decision = decide_live_priority_mode(&a, now, Some(90 * interval)).unwrap();
        assert!(decision.is_none());
    }
}
