use super::types::SupervisorArgs;
use super::validation::{
    next_arg, parse_positive_i64, parse_positive_u16, parse_positive_u64, parse_positive_usize,
    parse_symbols, parse_venues, validate_completed_args,
};
use std::error::Error;
use std::path::PathBuf;

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<SupervisorArgs>, Box<dyn Error>> {
    let mut parsed = SupervisorArgs::with_defaults();

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
                parsed.realtime_venues = vec![parsed.realtime_venue.clone()];
            }
            "--realtime-venues" => {
                parsed.realtime_venues = parse_venues(&next_arg(&mut args, "--realtime-venues")?)?;
                parsed.realtime_venue = parsed
                    .realtime_venues
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "binance".to_owned());
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
            "--live-nats-url" => {
                parsed.live_nats_url = Some(next_arg(&mut args, "--live-nats-url")?);
            }
            "--live-nats-stream" => {
                parsed.live_nats_stream = next_arg(&mut args, "--live-nats-stream")?;
            }
            "--live-nats-subject-prefix" => {
                parsed.live_nats_subject_prefix =
                    next_arg(&mut args, "--live-nats-subject-prefix")?;
            }
            "--live-nats-required" => {
                parsed.live_nats_required = true;
            }
            _ => return Err(format!("unknown supervisor argument: {arg}").into()),
        }
    }

    validate_completed_args(&mut parsed)?;
    Ok(Some(parsed))
}
