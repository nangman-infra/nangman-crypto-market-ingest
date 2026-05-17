use std::error::Error;
use std::path::PathBuf;

const DEFAULT_CONFIG_DIR: &str = "/opt/nangman-crypto/strategies/crypto/rust-engine/config";
const DEFAULT_L0_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
const DEFAULT_BINANCE_FUTURES_REST_BASE_URL: &str = "https://fapi.binance.com";
const DEFAULT_HIGH_WATER_PCT: u8 = 70;
const DEFAULT_EMERGENCY_PCT: u8 = 90;
const DEFAULT_SAFETY_FLOOR_HOURS: i64 = 2;
const DEFAULT_EVICTION_CHECK_INTERVAL_SECS: u64 = 600;
const DEFAULT_S3_RETENTION_DAYS: i64 = 45;
const DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS: u64 = 21_600;
const DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN: usize = 1_000;

#[derive(Debug)]
pub struct Args {
    pub venue: Venue,
    pub config_dir: PathBuf,
    pub duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub depth_snapshot_limit: u16,
    pub expect_symbol_count: usize,
    pub allow_partial_symbol_coverage: bool,
    pub binance_futures_rest_base_url: String,
    pub upbit_rest_base_url: Option<String>,
    pub upbit_websocket_url: Option<String>,
    pub upbit_quote_currency: String,
    pub upbit_orderbook_unit: u8,
    pub l0_s3_bucket: Option<String>,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
    pub local_disk_high_water_pct: u8,
    pub local_disk_emergency_pct: u8,
    pub safety_floor_hours: i64,
    pub eviction_check_interval_secs: u64,
    pub s3_retention_enabled: bool,
    pub s3_retention_days: i64,
    pub s3_retention_check_interval_secs: u64,
    pub s3_retention_max_deletes_per_run: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Binance,
    Upbit,
}

pub fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Option<Args>, Box<dyn Error>> {
    let mut parsed = Args {
        venue: Venue::Binance,
        config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
        duration_seconds: 15,
        log_interval_seconds: 5,
        depth_snapshot_limit: 100,
        expect_symbol_count: 50,
        allow_partial_symbol_coverage: false,
        binance_futures_rest_base_url: DEFAULT_BINANCE_FUTURES_REST_BASE_URL.to_owned(),
        upbit_rest_base_url: None,
        upbit_websocket_url: None,
        upbit_quote_currency: "KRW".to_owned(),
        upbit_orderbook_unit: 5,
        l0_s3_bucket: None,
        aws_profile: None,
        aws_region: DEFAULT_AWS_REGION.to_owned(),
        l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
        l0_flush_records: 1_000,
        l0_shard_count: 1,
        local_disk_high_water_pct: DEFAULT_HIGH_WATER_PCT,
        local_disk_emergency_pct: DEFAULT_EMERGENCY_PCT,
        safety_floor_hours: DEFAULT_SAFETY_FLOOR_HOURS,
        eviction_check_interval_secs: DEFAULT_EVICTION_CHECK_INTERVAL_SECS,
        s3_retention_enabled: true,
        s3_retention_days: DEFAULT_S3_RETENTION_DAYS,
        s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
        s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--venue" => {
                parsed.venue =
                    parse_venue(args.next().ok_or("--venue requires binance or upbit")?)?;
            }
            "--config" => {
                parsed.config_dir = PathBuf::from(
                    args.next()
                        .ok_or("--config requires an absolute config directory path")?,
                );
            }
            "--duration-seconds" => {
                parsed.duration_seconds = parse_positive_u64(
                    args.next()
                        .ok_or("--duration-seconds requires a positive integer")?,
                    "--duration-seconds",
                )?;
            }
            "--log-interval-seconds" => {
                parsed.log_interval_seconds = parse_positive_u64(
                    args.next()
                        .ok_or("--log-interval-seconds requires a positive integer")?,
                    "--log-interval-seconds",
                )?;
            }
            "--depth-snapshot-limit" => {
                parsed.depth_snapshot_limit = parse_depth_snapshot_limit(args.next())?;
            }
            "--expect-symbol-count" => {
                parsed.expect_symbol_count = parse_positive_usize(
                    args.next()
                        .ok_or("--expect-symbol-count requires a positive integer")?,
                    "--expect-symbol-count",
                )?;
            }
            "--allow-partial-symbol-coverage" => {
                parsed.allow_partial_symbol_coverage = true;
            }
            "--binance-futures-rest-base-url" => {
                parsed.binance_futures_rest_base_url = args
                    .next()
                    .ok_or("--binance-futures-rest-base-url requires an absolute HTTPS URL")?;
            }
            "--upbit-rest-base-url" => {
                parsed.upbit_rest_base_url = Some(
                    args.next()
                        .ok_or("--upbit-rest-base-url requires an absolute HTTPS URL")?,
                );
            }
            "--upbit-websocket-url" => {
                parsed.upbit_websocket_url = Some(
                    args.next()
                        .ok_or("--upbit-websocket-url requires an absolute WSS URL")?,
                );
            }
            "--upbit-quote-currency" => {
                parsed.upbit_quote_currency = args
                    .next()
                    .ok_or("--upbit-quote-currency requires a quote currency")?;
            }
            "--upbit-orderbook-unit" => {
                parsed.upbit_orderbook_unit = parse_upbit_orderbook_unit(args.next())?;
            }
            "--l0-s3-bucket" => {
                parsed.l0_s3_bucket = Some(args.next().ok_or("--l0-s3-bucket requires a bucket")?);
            }
            "--aws-profile" => {
                parsed.aws_profile = Some(args.next().ok_or("--aws-profile requires a profile")?);
            }
            "--aws-region" => {
                parsed.aws_region = args.next().ok_or("--aws-region requires a region")?;
            }
            "--l0-spool-root" => {
                parsed.l0_spool_root = PathBuf::from(
                    args.next()
                        .ok_or("--l0-spool-root requires an absolute directory path")?,
                );
            }
            "--l0-flush-records" => {
                parsed.l0_flush_records = parse_positive_usize(
                    args.next()
                        .ok_or("--l0-flush-records requires a positive integer")?,
                    "--l0-flush-records",
                )?;
            }
            "--l0-shard-count" => {
                parsed.l0_shard_count = parse_positive_u16(
                    args.next()
                        .ok_or("--l0-shard-count requires a positive integer")?,
                    "--l0-shard-count",
                )?;
            }
            "--local-disk-high-water-pct" => {
                parsed.local_disk_high_water_pct = parse_pct(
                    args.next()
                        .ok_or("--local-disk-high-water-pct requires 1..100")?,
                    "--local-disk-high-water-pct",
                )?;
            }
            "--local-disk-emergency-pct" => {
                parsed.local_disk_emergency_pct = parse_pct(
                    args.next()
                        .ok_or("--local-disk-emergency-pct requires 1..100")?,
                    "--local-disk-emergency-pct",
                )?;
            }
            "--safety-floor-hours" => {
                parsed.safety_floor_hours = parse_positive_i64(
                    args.next()
                        .ok_or("--safety-floor-hours requires a positive integer")?,
                    "--safety-floor-hours",
                )?;
            }
            "--eviction-check-interval-secs" => {
                parsed.eviction_check_interval_secs = parse_positive_u64(
                    args.next()
                        .ok_or("--eviction-check-interval-secs requires a positive integer")?,
                    "--eviction-check-interval-secs",
                )?;
            }
            "--disable-s3-retention" => {
                parsed.s3_retention_enabled = false;
            }
            "--s3-retention-days" => {
                parsed.s3_retention_days = parse_positive_i64(
                    args.next()
                        .ok_or("--s3-retention-days requires a positive integer")?,
                    "--s3-retention-days",
                )?;
            }
            "--s3-retention-check-interval-secs" => {
                parsed.s3_retention_check_interval_secs = parse_positive_u64(
                    args.next()
                        .ok_or("--s3-retention-check-interval-secs requires a positive integer")?,
                    "--s3-retention-check-interval-secs",
                )?;
            }
            "--s3-retention-max-deletes-per-run" => {
                parsed.s3_retention_max_deletes_per_run = parse_positive_usize(
                    args.next()
                        .ok_or("--s3-retention-max-deletes-per-run requires a positive integer")?,
                    "--s3-retention-max-deletes-per-run",
                )?;
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if parsed.local_disk_emergency_pct < parsed.local_disk_high_water_pct {
        return Err("--local-disk-emergency-pct must be >= --local-disk-high-water-pct".into());
    }

    if parsed.log_interval_seconds > parsed.duration_seconds {
        parsed.log_interval_seconds = parsed.duration_seconds;
    }
    Ok(Some(parsed))
}

fn parse_venue(value: String) -> Result<Venue, Box<dyn Error>> {
    match value.as_str() {
        "binance" => Ok(Venue::Binance),
        "upbit" => Ok(Venue::Upbit),
        _ => Err("--venue must be binance or upbit".into()),
    }
}

fn parse_depth_snapshot_limit(value: Option<String>) -> Result<u16, Box<dyn Error>> {
    let parsed = value
        .ok_or("--depth-snapshot-limit requires 5, 10, 20, 50, 100, 500, 1000, or 5000")?
        .parse::<u16>()
        .map_err(|_| "--depth-snapshot-limit must be an integer")?;
    if !matches!(parsed, 5 | 10 | 20 | 50 | 100 | 500 | 1000 | 5000) {
        return Err("--depth-snapshot-limit must be 5, 10, 20, 50, 100, 500, 1000, or 5000".into());
    }
    Ok(parsed)
}

fn parse_upbit_orderbook_unit(value: Option<String>) -> Result<u8, Box<dyn Error>> {
    let parsed = value
        .ok_or("--upbit-orderbook-unit requires 1, 5, 15, or 30")?
        .parse::<u8>()
        .map_err(|_| "--upbit-orderbook-unit must be an integer")?;
    if !matches!(parsed, 1 | 5 | 15 | 30) {
        return Err("--upbit-orderbook-unit must be 1, 5, 15, or 30".into());
    }
    Ok(parsed)
}

fn parse_positive_u64(value: String, name: &str) -> Result<u64, Box<dyn Error>> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_positive_usize(value: String, name: &str) -> Result<usize, Box<dyn Error>> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_positive_u16(value: String, name: &str) -> Result<u16, Box<dyn Error>> {
    let parsed = value
        .parse::<u16>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_positive_i64(value: String, name: &str) -> Result<i64, Box<dyn Error>> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

fn parse_pct(value: String, name: &str) -> Result<u8, Box<dyn Error>> {
    let parsed = value
        .parse::<u8>()
        .map_err(|_| format!("{name} must be 1..100"))?;
    if parsed == 0 || parsed > 100 {
        return Err(format!("{name} must be 1..100").into());
    }
    Ok(parsed)
}

pub fn print_help() {
    println!(
        "market-ingest-app\n\
         Usage:\n\
           cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml -- \\\n\
             --venue binance \\\n\
             --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \\\n\
             --duration-seconds 15 \\\n\
             --log-interval-seconds 5 \\\n\
             --depth-snapshot-limit 100 \\\n\
             --binance-futures-rest-base-url https://fapi.binance.com \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214\n\
          cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml -- \\\n\
             --venue upbit \\\n\
             --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \\\n\
             --duration-seconds 15 \\\n\
             --log-interval-seconds 5 \\\n\
             --expect-symbol-count 50 \\\n\
             --upbit-orderbook-unit 5 \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214\n\
         \n\
         This reads Binance or Upbit public WebSocket streams only. It does not use private APIs,\n\
         credentials, AI hot-path decisions, order placement, or live trading.\n\
         S3 retention cleanup is app-owned when --l0-s3-bucket is set. L0 defaults to 45 days;\n\
         bucket lifecycle remains a fallback safety net at 60 days."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Vec<String> {
        vec!["--venue".to_owned(), "binance".to_owned()]
    }

    #[test]
    fn defaults_eviction_knobs() {
        let parsed = parse_args(base_args().into_iter()).unwrap().unwrap();
        assert_eq!(parsed.local_disk_high_water_pct, 70);
        assert_eq!(parsed.local_disk_emergency_pct, 90);
        assert_eq!(parsed.safety_floor_hours, 2);
        assert_eq!(parsed.eviction_check_interval_secs, 600);
        assert_eq!(parsed.s3_retention_days, 45);
        assert_eq!(parsed.s3_retention_check_interval_secs, 21_600);
        assert_eq!(parsed.s3_retention_max_deletes_per_run, 1_000);
        assert_eq!(
            parsed.binance_futures_rest_base_url,
            "https://fapi.binance.com"
        );
    }

    #[test]
    fn rejects_emergency_below_high_water() {
        let mut raw = base_args();
        raw.push("--local-disk-high-water-pct".to_owned());
        raw.push("80".to_owned());
        raw.push("--local-disk-emergency-pct".to_owned());
        raw.push("70".to_owned());
        let err = parse_args(raw.into_iter()).err().unwrap();
        assert!(err.to_string().contains(">="));
    }

    #[test]
    fn rejects_pct_above_100() {
        let mut raw = base_args();
        raw.push("--local-disk-high-water-pct".to_owned());
        raw.push("150".to_owned());
        let err = parse_args(raw.into_iter()).err().unwrap();
        assert!(err.to_string().contains("1..100"));
    }

    #[test]
    fn parses_eviction_knobs_when_provided() {
        let mut raw = base_args();
        raw.push("--local-disk-high-water-pct".to_owned());
        raw.push("60".to_owned());
        raw.push("--local-disk-emergency-pct".to_owned());
        raw.push("85".to_owned());
        raw.push("--safety-floor-hours".to_owned());
        raw.push("4".to_owned());
        raw.push("--eviction-check-interval-secs".to_owned());
        raw.push("300".to_owned());
        raw.push("--s3-retention-days".to_owned());
        raw.push("365".to_owned());
        raw.push("--s3-retention-check-interval-secs".to_owned());
        raw.push("3600".to_owned());
        raw.push("--s3-retention-max-deletes-per-run".to_owned());
        raw.push("50".to_owned());
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(parsed.local_disk_high_water_pct, 60);
        assert_eq!(parsed.local_disk_emergency_pct, 85);
        assert_eq!(parsed.safety_floor_hours, 4);
        assert_eq!(parsed.eviction_check_interval_secs, 300);
        assert_eq!(parsed.s3_retention_days, 365);
        assert_eq!(parsed.s3_retention_check_interval_secs, 3600);
        assert_eq!(parsed.s3_retention_max_deletes_per_run, 50);
    }
}
