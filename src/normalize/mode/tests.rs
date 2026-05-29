use super::*;
use crate::normalize::args::NormalizeArgs;
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
        schedule_interval_ms: 900_000,
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
        live_priority_only: false,
        live_priority_lag_threshold_ms: 900_000,
        s3_retention_enabled: true,
        l0_s3_retention_days: 45,
        l1_s3_retention_days: 240,
        s3_retention_check_interval_secs: 21_600,
        s3_retention_max_deletes_per_run: 1_000,
        l1_index_upload_concurrency: 1,
    }
}

#[test]
fn returns_none_before_watermark() {
    let now = 900_000 + 360_000 - 1;
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
    let last_success = 99 * interval;
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
    let last_success = (1_000 - 28) * interval;
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
    let last_success = 96 * interval;
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
    let last_success = 96 * interval;
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
    let oldest_l0 = (1_000 - 50) * interval + 100;
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
