use std::error::Error;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
const DEFAULT_L0_LOCAL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
const DEFAULT_L1_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l1";
const DEFAULT_CATCHUP_TMP_ROOT: &str = "/opt/nangman-crypto/data/spool/market-normalize/catchup";
const DEFAULT_WINDOW_MS: i64 = 1_000;
const DEFAULT_SCHEDULE_INTERVAL_MS: i64 = 900_000;
const DEFAULT_SCAN_MARGIN_MS: i64 = 300_000;
const DEFAULT_PROJECTION_LOOKBACK_MS: i64 = 3_600_000;
const DEFAULT_WATERMARK_DELAY_MS: i64 = 360_000;
const DEFAULT_CLOCK_SKEW_MARGIN_MS: i64 = 1_000;
const DEFAULT_MAX_LATENCY_MS: i64 = 1_000;
const DEFAULT_MAX_WINDOWS_PER_TICK: usize = 192;
const DEFAULT_L0_RUN_KEY_OVERLAP_MS: i64 = 360_000;
const DEFAULT_LIVE_PRIORITY_LAG_THRESHOLD_MS: i64 = 900_000;
const DEFAULT_S3_RETENTION_DAYS: i64 = 240;
const DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS: u64 = 21_600;
const DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN: usize = 1_000;

#[derive(Debug, Clone)]
pub struct NormalizeArgs {
    pub l0_s3_bucket: String,
    pub l0_local_root: PathBuf,
    pub l1_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub input_start_ms: Option<i64>,
    pub input_end_ms: Option<i64>,
    pub schedule_interval_ms: i64,
    pub window_ms: i64,
    pub scan_margin_ms: i64,
    pub projection_lookback_ms: i64,
    pub watermark_delay_ms: i64,
    pub clock_skew_margin_ms: i64,
    pub max_latency_ms: i64,
    pub l0_run_key_overlap_ms: i64,
    pub spool_root: PathBuf,
    pub catchup_tmp_root: PathBuf,
    pub preflight: bool,
    pub audit_l1_index_start_ms: Option<i64>,
    pub audit_l1_index_end_ms: Option<i64>,
    pub max_windows_per_tick: usize,
    pub live_priority: bool,
    pub live_priority_lag_threshold_ms: i64,
    pub s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<NormalizeArgs>, Box<dyn Error>> {
    let mut parsed = NormalizeArgs {
        l0_s3_bucket: String::new(),
        l0_local_root: PathBuf::from(DEFAULT_L0_LOCAL_ROOT),
        l1_s3_bucket: String::new(),
        aws_profile: None,
        aws_region: DEFAULT_AWS_REGION.to_owned(),
        input_start_ms: None,
        input_end_ms: None,
        schedule_interval_ms: DEFAULT_SCHEDULE_INTERVAL_MS,
        window_ms: DEFAULT_WINDOW_MS,
        scan_margin_ms: DEFAULT_SCAN_MARGIN_MS,
        projection_lookback_ms: DEFAULT_PROJECTION_LOOKBACK_MS,
        watermark_delay_ms: DEFAULT_WATERMARK_DELAY_MS,
        clock_skew_margin_ms: DEFAULT_CLOCK_SKEW_MARGIN_MS,
        max_latency_ms: DEFAULT_MAX_LATENCY_MS,
        l0_run_key_overlap_ms: DEFAULT_L0_RUN_KEY_OVERLAP_MS,
        spool_root: PathBuf::from(DEFAULT_L1_SPOOL_ROOT),
        catchup_tmp_root: PathBuf::from(DEFAULT_CATCHUP_TMP_ROOT),
        preflight: false,
        audit_l1_index_start_ms: None,
        audit_l1_index_end_ms: None,
        max_windows_per_tick: DEFAULT_MAX_WINDOWS_PER_TICK,
        live_priority: false,
        live_priority_lag_threshold_ms: DEFAULT_LIVE_PRIORITY_LAG_THRESHOLD_MS,
        s3_retention_days: DEFAULT_S3_RETENTION_DAYS,
        s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
        s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
    };

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
            "--live-priority-lag-threshold-ms" => {
                parsed.live_priority_lag_threshold_ms =
                    parse_positive_i64(args.next(), "--live-priority-lag-threshold-ms")?;
            }
            "--s3-retention-days" => {
                parsed.s3_retention_days = parse_positive_i64(args.next(), "--s3-retention-days")?;
            }
            "--s3-retention-check-interval-secs" => {
                parsed.s3_retention_check_interval_secs =
                    parse_positive_u64(args.next(), "--s3-retention-check-interval-secs")?;
            }
            "--s3-retention-max-deletes-per-run" => {
                parsed.s3_retention_max_deletes_per_run =
                    parse_positive_usize(args.next(), "--s3-retention-max-deletes-per-run")?;
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    if parsed.l0_s3_bucket.is_empty() {
        return Err("--l0-s3-bucket is required".into());
    }
    if parsed.l1_s3_bucket.is_empty() {
        return Err("--l1-s3-bucket is required".into());
    }
    if parsed.watermark_delay_ms < parsed.scan_margin_ms {
        return Err("--watermark-delay-ms must be >= --scan-margin-ms".into());
    }
    if (parsed.input_start_ms.is_some()) != (parsed.input_end_ms.is_some()) {
        return Err("--input-start-ms and --input-end-ms must be provided together".into());
    }
    if (parsed.audit_l1_index_start_ms.is_some()) != (parsed.audit_l1_index_end_ms.is_some()) {
        return Err(
            "--audit-l1-index-start-ms and --audit-l1-index-end-ms must be provided together"
                .into(),
        );
    }
    Ok(Some(parsed))
}

pub fn print_help() {
    println!(
        r#"market-normalize
Usage:
  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
    --l0-local-root /opt/nangman-crypto/data/spool/market-ingest/l0 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214 \
    --catchup-tmp-root /opt/nangman-crypto/data/spool/market-normalize/catchup \
    --aws-profile market-ingest-roles-anywhere

  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
    --l0-local-root /opt/nangman-crypto/data/spool/market-ingest/l0 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214 \
    --catchup-tmp-root /opt/nangman-crypto/data/spool/market-normalize/catchup \
    --input-start-ms 1778042400000 \
    --input-end-ms 1778043300000

  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214 \
    --preflight

  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214 \
    --audit-l1-index-start-ms 1778042400000 \
    --audit-l1-index-end-ms 1778043300000

Without an explicit input range, market-normalize runs as a long-lived worker.
Each tick decides LIVE / CATCH-UP from the most recent successful L1 manifest
and the watermark, then processes up to --max-windows-per-tick contiguous
windows before sleeping for --schedule-interval-ms. Events with
ingest_timestamp_ms - exchange_timestamp_ms greater than --max-latency-ms are
counted as delayed. L0 object keys are filtered by run_id timestamp using
--l0-run-key-overlap-ms; keep this value greater than or equal to the L0 ingest
duration to avoid dropping boundary files. --live-priority processes the latest
closed watermark window first when sequential catch-up lags by at least
--live-priority-lag-threshold-ms, then continues the same tick with contiguous
catch-up work. With an explicit range, BACKFILL mode is one-shot. --preflight
and --audit-l1-index-* are also one-shot. S3 retention cleanup is app-owned
for both L0 and L1 buckets in long-lived worker mode; bucket lifecycle remains
only a fallback safety net."#
    );
}

pub fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn parse_i64_arg(value: Option<String>, name: &str) -> Result<i64, Box<dyn Error>> {
    value
        .ok_or_else(|| format!("{name} requires an integer"))?
        .parse::<i64>()
        .map_err(|_| format!("{name} must be an integer").into())
}

fn parse_positive_i64(value: Option<String>, name: &str) -> Result<i64, Box<dyn Error>> {
    let parsed = parse_i64_arg(value, name)?;
    if parsed <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: Option<String>, name: &str) -> Result<u64, Box<dyn Error>> {
    let parsed = value
        .ok_or_else(|| format!("{name} requires a positive integer"))?
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_positive_usize(value: Option<String>, name: &str) -> Result<usize, Box<dyn Error>> {
    let parsed = value
        .ok_or_else(|| format!("{name} requires a positive integer"))?
        .parse::<usize>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimum_required_flags() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.l0_s3_bucket, "l0");
        assert_eq!(parsed.l1_s3_bucket, "l1");
        assert_eq!(parsed.s3_retention_days, 240);
        assert_eq!(parsed.s3_retention_check_interval_secs, 21_600);
        assert_eq!(parsed.s3_retention_max_deletes_per_run, 1_000);
    }

    #[test]
    fn requires_input_start_and_end_together() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--input-start-ms".to_owned(),
            "1000".to_owned(),
        ];
        let err = parse_args(raw.into_iter()).err().unwrap();
        assert!(err.to_string().contains("must be provided together"));
    }

    #[test]
    fn rejects_unknown_flag() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--unknown".to_owned(),
        ];
        assert!(parse_args(raw.into_iter()).is_err());
    }

    #[test]
    fn parses_preflight_flag() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--preflight".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert!(parsed.preflight);
    }

    #[test]
    fn parses_l1_index_audit_range() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--audit-l1-index-start-ms".to_owned(),
            "1000".to_owned(),
            "--audit-l1-index-end-ms".to_owned(),
            "2000".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.audit_l1_index_start_ms, Some(1000));
        assert_eq!(parsed.audit_l1_index_end_ms, Some(2000));
    }

    #[test]
    fn requires_l1_index_audit_range_pair() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--audit-l1-index-start-ms".to_owned(),
            "1000".to_owned(),
        ];

        let err = parse_args(raw.into_iter()).err().unwrap();
        assert!(err.to_string().contains("must be provided together"));
    }

    #[test]
    fn parses_max_windows_per_tick() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--max-windows-per-tick".to_owned(),
            "12".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.max_windows_per_tick, 12);
    }

    #[test]
    fn parses_max_latency_ms() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--max-latency-ms".to_owned(),
            "2500".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.max_latency_ms, 2500);
    }

    #[test]
    fn parses_l0_run_key_overlap_ms() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--l0-run-key-overlap-ms".to_owned(),
            "420000".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.l0_run_key_overlap_ms, 420000);
    }

    #[test]
    fn accepts_legacy_max_windows_per_invocation_alias() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--max-windows-per-invocation".to_owned(),
            "12".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.max_windows_per_tick, 12);
    }

    #[test]
    fn parses_live_priority_knobs() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--live-priority".to_owned(),
            "--live-priority-lag-threshold-ms".to_owned(),
            "1800000".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert!(parsed.live_priority);
        assert_eq!(parsed.live_priority_lag_threshold_ms, 1_800_000);
    }

    #[test]
    fn parses_s3_retention_knobs() {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
            "--s3-retention-days".to_owned(),
            "365".to_owned(),
            "--s3-retention-check-interval-secs".to_owned(),
            "3600".to_owned(),
            "--s3-retention-max-deletes-per-run".to_owned(),
            "50".to_owned(),
        ];
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.s3_retention_days, 365);
        assert_eq!(parsed.s3_retention_check_interval_secs, 3600);
        assert_eq!(parsed.s3_retention_max_deletes_per_run, 50);
    }
}
