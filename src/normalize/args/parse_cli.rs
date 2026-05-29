use std::error::Error;
use std::path::PathBuf;

use super::NormalizeArgs;
use super::parse::{parse_i64_arg, parse_positive_i64, parse_positive_u64, parse_positive_usize};
use super::validation::validate_parsed_args;

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<NormalizeArgs>, Box<dyn Error>> {
    let mut parsed = NormalizeArgs::with_defaults();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--l0-s3-bucket" => {
                parsed.l0_s3_bucket = args.next().ok_or("--l0-s3-bucket requires a bucket")?;
            }
            "--l0-local-root" => {
                parsed.l0_local_root = PathBuf::from(
                    args.next()
                        .ok_or("--l0-local-root requires an absolute directory path")?,
                );
            }
            "--l1-s3-bucket" => {
                parsed.l1_s3_bucket = args.next().ok_or("--l1-s3-bucket requires a bucket")?;
            }
            "--aws-profile" => {
                parsed.aws_profile = Some(args.next().ok_or("--aws-profile requires a profile")?);
            }
            "--aws-region" => {
                parsed.aws_region = args.next().ok_or("--aws-region requires a region")?;
            }
            "--input-start-ms" => {
                parsed.input_start_ms = Some(parse_i64_arg(args.next(), "--input-start-ms")?);
            }
            "--input-end-ms" => {
                parsed.input_end_ms = Some(parse_i64_arg(args.next(), "--input-end-ms")?);
            }
            "--schedule-interval-ms" => {
                parsed.schedule_interval_ms =
                    parse_positive_i64(args.next(), "--schedule-interval-ms")?;
            }
            "--window-ms" => {
                parsed.window_ms = parse_positive_i64(args.next(), "--window-ms")?;
            }
            "--scan-margin-ms" => {
                parsed.scan_margin_ms = parse_positive_i64(args.next(), "--scan-margin-ms")?;
            }
            "--projection-lookback-ms" => {
                parsed.projection_lookback_ms =
                    parse_positive_i64(args.next(), "--projection-lookback-ms")?;
            }
            "--watermark-delay-ms" => {
                parsed.watermark_delay_ms =
                    parse_positive_i64(args.next(), "--watermark-delay-ms")?;
            }
            "--clock-skew-margin-ms" => {
                parsed.clock_skew_margin_ms =
                    parse_positive_i64(args.next(), "--clock-skew-margin-ms")?;
            }
            "--max-latency-ms" => {
                parsed.max_latency_ms = parse_positive_i64(args.next(), "--max-latency-ms")?;
            }
            "--l0-run-key-overlap-ms" => {
                parsed.l0_run_key_overlap_ms =
                    parse_positive_i64(args.next(), "--l0-run-key-overlap-ms")?;
            }
            "--spool-root" => {
                parsed.spool_root = PathBuf::from(
                    args.next()
                        .ok_or("--spool-root requires an absolute directory path")?,
                );
            }
            "--catchup-tmp-root" => {
                parsed.catchup_tmp_root = PathBuf::from(
                    args.next()
                        .ok_or("--catchup-tmp-root requires an absolute directory path")?,
                );
            }
            "--preflight" => {
                parsed.preflight = true;
            }
            "--audit-l1-index-start-ms" => {
                parsed.audit_l1_index_start_ms =
                    Some(parse_i64_arg(args.next(), "--audit-l1-index-start-ms")?);
            }
            "--audit-l1-index-end-ms" => {
                parsed.audit_l1_index_end_ms =
                    Some(parse_i64_arg(args.next(), "--audit-l1-index-end-ms")?);
            }
            "--max-windows-per-tick" => {
                parsed.max_windows_per_tick =
                    parse_positive_usize(args.next(), "--max-windows-per-tick")?;
            }
            "--max-windows-per-invocation" => {
                parsed.max_windows_per_tick =
                    parse_positive_usize(args.next(), "--max-windows-per-invocation")?;
            }
            "--live-priority" => {
                parsed.live_priority = true;
            }
            "--live-priority-only" => {
                parsed.live_priority = true;
                parsed.live_priority_only = true;
            }
            "--live-priority-lag-threshold-ms" => {
                parsed.live_priority_lag_threshold_ms =
                    parse_positive_i64(args.next(), "--live-priority-lag-threshold-ms")?;
            }
            "--disable-s3-retention" => {
                parsed.s3_retention_enabled = false;
            }
            "--s3-retention-days" => {
                let retention_days = parse_positive_i64(args.next(), "--s3-retention-days")?;
                parsed.l0_s3_retention_days = retention_days;
                parsed.l1_s3_retention_days = retention_days;
            }
            "--l0-s3-retention-days" => {
                parsed.l0_s3_retention_days =
                    parse_positive_i64(args.next(), "--l0-s3-retention-days")?;
            }
            "--l1-s3-retention-days" => {
                parsed.l1_s3_retention_days =
                    parse_positive_i64(args.next(), "--l1-s3-retention-days")?;
            }
            "--s3-retention-check-interval-secs" => {
                parsed.s3_retention_check_interval_secs =
                    parse_positive_u64(args.next(), "--s3-retention-check-interval-secs")?;
            }
            "--s3-retention-max-deletes-per-run" => {
                parsed.s3_retention_max_deletes_per_run =
                    parse_positive_usize(args.next(), "--s3-retention-max-deletes-per-run")?;
            }
            "--l1-index-upload-concurrency" => {
                parsed.l1_index_upload_concurrency =
                    parse_positive_usize(args.next(), "--l1-index-upload-concurrency")?;
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    validate_parsed_args(&parsed)?;
    Ok(Some(parsed))
}
