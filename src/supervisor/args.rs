use std::error::Error;
use std::path::PathBuf;

const DEFAULT_CONFIG_DIR: &str = "/opt/nangman-crypto/strategies/crypto/rust-engine/config";
const DEFAULT_L0_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
const DEFAULT_L1_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l1";
const DEFAULT_CATCHUP_TMP_ROOT: &str = "/opt/nangman-crypto/data/spool/market-normalize/catchup";
const DEFAULT_REALTIME_BIN: &str = "/usr/local/bin/market-ingest-app";
const DEFAULT_BACKFILL_BIN: &str = "/usr/local/bin/market-backfill";
const DEFAULT_NORMALIZE_BIN: &str = "/usr/local/bin/market-normalize";
const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
pub(super) const DEFAULT_L0_S3_BUCKET: &str =
    "nangman-crypto-dev-market-ingest-l0-<account-suffix>";
pub(super) const DEFAULT_L1_S3_BUCKET: &str =
    "nangman-crypto-dev-market-ingest-l1-<account-suffix>";
const DEFAULT_RESTART_DELAY_SECS: u64 = 15;
const DEFAULT_BOOTSTRAP_LOOKBACK_DAYS: i64 = 210;
const DEFAULT_BOOTSTRAP_CHUNK_HOURS: i64 = 24;
const DEFAULT_BOOTSTRAP_INTERVAL_SECS: u64 = 60;
const DEFAULT_REALTIME_DURATION_SECONDS: u64 = 31_536_000;
pub(super) const DEFAULT_L0_RUN_KEY_OVERLAP_MS: i64 = 360_000;
const DEFAULT_L0_S3_RETENTION_DAYS: i64 = 45;
const DEFAULT_L1_S3_RETENTION_DAYS: i64 = 240;
const DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS: u64 = 21_600;
const DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN: usize = 1_000;

#[derive(Debug, Clone)]
pub struct SupervisorArgs {
    pub config_dir: PathBuf,
    pub l0_s3_bucket: String,
    pub l1_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l1_spool_root: PathBuf,
    pub catchup_tmp_root: PathBuf,
    pub realtime_bin: PathBuf,
    pub backfill_bin: PathBuf,
    pub normalize_bin: PathBuf,
    pub realtime_venue: String,
    pub expect_symbol_count: usize,
    pub realtime_duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
    pub bootstrap_enabled: bool,
    pub bootstrap_lookback_days: i64,
    pub bootstrap_chunk_hours: i64,
    pub bootstrap_interval_secs: u64,
    pub bootstrap_symbols: Option<Vec<String>>,
    pub normalize_schedule_interval_ms: i64,
    pub l0_run_key_overlap_ms: i64,
    pub normalize_max_windows_per_tick: usize,
    pub l0_s3_retention_days: i64,
    pub l1_s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
    pub restart_delay_secs: u64,
}

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<SupervisorArgs>, Box<dyn Error>> {
    let mut parsed = SupervisorArgs {
        config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
        l0_s3_bucket: String::new(),
        l1_s3_bucket: String::new(),
        aws_profile: None,
        aws_region: DEFAULT_AWS_REGION.to_owned(),
        l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
        l1_spool_root: PathBuf::from(DEFAULT_L1_SPOOL_ROOT),
        catchup_tmp_root: PathBuf::from(DEFAULT_CATCHUP_TMP_ROOT),
        realtime_bin: PathBuf::from(DEFAULT_REALTIME_BIN),
        backfill_bin: PathBuf::from(DEFAULT_BACKFILL_BIN),
        normalize_bin: PathBuf::from(DEFAULT_NORMALIZE_BIN),
        realtime_venue: "binance".to_owned(),
        expect_symbol_count: 50,
        realtime_duration_seconds: DEFAULT_REALTIME_DURATION_SECONDS,
        log_interval_seconds: 30,
        l0_flush_records: 1_000,
        l0_shard_count: 1,
        bootstrap_enabled: true,
        bootstrap_lookback_days: DEFAULT_BOOTSTRAP_LOOKBACK_DAYS,
        bootstrap_chunk_hours: DEFAULT_BOOTSTRAP_CHUNK_HOURS,
        bootstrap_interval_secs: DEFAULT_BOOTSTRAP_INTERVAL_SECS,
        bootstrap_symbols: None,
        normalize_schedule_interval_ms: 900_000,
        l0_run_key_overlap_ms: DEFAULT_L0_RUN_KEY_OVERLAP_MS,
        normalize_max_windows_per_tick: 192,
        l0_s3_retention_days: DEFAULT_L0_S3_RETENTION_DAYS,
        l1_s3_retention_days: DEFAULT_L1_S3_RETENTION_DAYS,
        s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
        s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
        restart_delay_secs: DEFAULT_RESTART_DELAY_SECS,
    };

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
            _ => return Err(format!("unknown supervisor argument: {arg}").into()),
        }
    }

    if parsed.realtime_venue != "binance" && parsed.realtime_venue != "upbit" {
        return Err("--realtime-venue must be binance or upbit".into());
    }
    validate_bucket_arg(&parsed.l0_s3_bucket, "--l0-s3-bucket")?;
    validate_bucket_arg(&parsed.l1_s3_bucket, "--l1-s3-bucket")?;
    if parsed.bootstrap_lookback_days > 0 && parsed.bootstrap_chunk_hours > 24 {
        return Err("--bootstrap-chunk-hours must be <= 24 to keep recovery chunks bounded".into());
    }
    if 24 % parsed.bootstrap_chunk_hours != 0 {
        return Err(
            "--bootstrap-chunk-hours must evenly divide 24 for stable UTC day partitions".into(),
        );
    }
    Ok(Some(parsed))
}

pub fn print_help() {
    println!(
        r#"crypto-market-ingest-supervisor
Usage:
  crypto-market-ingest-supervisor \
    --l0-s3-bucket {} \
    --l1-s3-bucket {}

Runs the all-in-one market data service:
  1. realtime L0 ingest
  2. historical bootstrap backfill
  3. long-lived L1 normalization

The ECS service should run this supervisor as the only container entrypoint."#,
        DEFAULT_L0_S3_BUCKET, DEFAULT_L1_S3_BUCKET
    );
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn validate_bucket_arg(value: &str, name: &str) -> Result<(), Box<dyn Error>> {
    if value.trim().is_empty() {
        return Err(format!("{name} is required").into());
    }
    if value.contains('<') || value.contains('>') {
        return Err(
            format!("{name} must be a real bucket name, not a public-doc placeholder").into(),
        );
    }
    Ok(())
}

fn parse_positive_i64(value: String) -> Result<i64, Box<dyn Error>> {
    let parsed = value.parse::<i64>()?;
    if parsed <= 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: String) -> Result<u64, Box<dyn Error>> {
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_positive_usize(value: String) -> Result<usize, Box<dyn Error>> {
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_positive_u16(value: String) -> Result<u16, Box<dyn Error>> {
    let parsed = value.parse::<u16>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

fn parse_symbols(value: &str, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let symbols = value
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(format!("{name} requires at least one symbol").into());
    }
    Ok(symbols)
}
