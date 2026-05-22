use super::BootstrapChunk;
use super::SupervisorArgs;

pub(super) fn realtime_args(args: &SupervisorArgs) -> Vec<String> {
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

pub(super) fn backfill_args(args: &SupervisorArgs, chunk: &BootstrapChunk) -> Vec<String> {
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

pub(super) fn normalize_args(args: &SupervisorArgs) -> Vec<String> {
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

pub(super) fn live_priority_normalize_args(args: &SupervisorArgs) -> Vec<String> {
    let mut values = normalize_args(args);
    values.extend(["--max-windows-per-tick".to_owned(), "1".to_owned()]);
    values
}

pub(super) fn normalize_backfill_args(
    args: &SupervisorArgs,
    chunk: &BootstrapChunk,
) -> Vec<String> {
    let mut values = normalize_args(args);
    values.extend([
        "--input-start-ms".to_owned(),
        chunk.start_ms.to_string(),
        "--input-end-ms".to_owned(),
        chunk.end_ms.to_string(),
    ]);
    values
}
