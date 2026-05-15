use super::BackfillError;
use std::path::PathBuf;

const DEFAULT_CONFIG_DIR: &str = "/opt/nangman-crypto/strategies/crypto/rust-engine/config";
const DEFAULT_L0_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
const DEFAULT_UPBIT_QUOTE_CURRENCY: &str = "KRW";

#[derive(Debug, Clone)]
pub struct BackfillArgs {
    pub venue: Venue,
    pub config_dir: PathBuf,
    pub rest_base_url: Option<String>,
    pub input_start_ms: i64,
    pub input_end_ms: i64,
    pub expect_symbol_count: usize,
    pub symbols: Option<Vec<String>>,
    pub upbit_quote_currency: String,
    pub l0_s3_bucket: String,
    pub aws_profile: Option<String>,
    pub aws_region: String,
    pub l0_spool_root: PathBuf,
    pub l0_flush_records: usize,
    pub l0_shard_count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Binance,
    Upbit,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Upbit => "upbit",
        }
    }
}

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<BackfillArgs>, BackfillError> {
    let mut parsed = BackfillArgs {
        venue: Venue::Binance,
        config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
        rest_base_url: None,
        input_start_ms: 0,
        input_end_ms: 0,
        expect_symbol_count: 50,
        symbols: None,
        upbit_quote_currency: DEFAULT_UPBIT_QUOTE_CURRENCY.to_owned(),
        l0_s3_bucket: String::new(),
        aws_profile: None,
        aws_region: DEFAULT_AWS_REGION.to_owned(),
        l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
        l0_flush_records: 1_000,
        l0_shard_count: 1,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--venue" => {
                parsed.venue = parse_venue(args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs("--venue requires a value".to_owned())
                })?)?;
            }
            "--config" => {
                parsed.config_dir = PathBuf::from(args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs(
                        "--config requires an absolute config directory path".to_owned(),
                    )
                })?);
            }
            "--rest-base-url" => {
                parsed.rest_base_url = Some(args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs("--rest-base-url requires an HTTPS URL".to_owned())
                })?);
            }
            "--input-start-ms" => {
                parsed.input_start_ms = parse_positive_i64(
                    args.next().ok_or_else(|| {
                        BackfillError::InvalidArgs(
                            "--input-start-ms requires a positive integer".to_owned(),
                        )
                    })?,
                    "--input-start-ms",
                )?;
            }
            "--input-end-ms" => {
                parsed.input_end_ms = parse_positive_i64(
                    args.next().ok_or_else(|| {
                        BackfillError::InvalidArgs(
                            "--input-end-ms requires a positive integer".to_owned(),
                        )
                    })?,
                    "--input-end-ms",
                )?;
            }
            "--expect-symbol-count" => {
                parsed.expect_symbol_count = parse_positive_usize(
                    args.next().ok_or_else(|| {
                        BackfillError::InvalidArgs(
                            "--expect-symbol-count requires a positive integer".to_owned(),
                        )
                    })?,
                    "--expect-symbol-count",
                )?;
            }
            "--symbols" => {
                let raw = args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs(
                        "--symbols requires comma-separated symbols".to_owned(),
                    )
                })?;
                parsed.symbols = Some(parse_symbols(&raw)?);
            }
            "--upbit-quote-currency" => {
                parsed.upbit_quote_currency = args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs(
                        "--upbit-quote-currency requires a quote currency".to_owned(),
                    )
                })?;
            }
            "--l0-s3-bucket" => {
                parsed.l0_s3_bucket = args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs("--l0-s3-bucket requires a bucket".to_owned())
                })?;
            }
            "--aws-profile" => {
                parsed.aws_profile = Some(args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs("--aws-profile requires a profile".to_owned())
                })?);
            }
            "--aws-region" => {
                parsed.aws_region = args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs("--aws-region requires a region".to_owned())
                })?;
            }
            "--l0-spool-root" => {
                parsed.l0_spool_root = PathBuf::from(args.next().ok_or_else(|| {
                    BackfillError::InvalidArgs(
                        "--l0-spool-root requires an absolute directory path".to_owned(),
                    )
                })?);
            }
            "--l0-flush-records" => {
                parsed.l0_flush_records = parse_positive_usize(
                    args.next().ok_or_else(|| {
                        BackfillError::InvalidArgs(
                            "--l0-flush-records requires a positive integer".to_owned(),
                        )
                    })?,
                    "--l0-flush-records",
                )?;
            }
            "--l0-shard-count" => {
                parsed.l0_shard_count = parse_positive_u16(
                    args.next().ok_or_else(|| {
                        BackfillError::InvalidArgs(
                            "--l0-shard-count requires a positive integer".to_owned(),
                        )
                    })?,
                    "--l0-shard-count",
                )?;
            }
            _ => {
                return Err(BackfillError::InvalidArgs(format!(
                    "unknown argument: {arg}"
                )));
            }
        }
    }

    if parsed.input_start_ms == 0 {
        return Err(BackfillError::InvalidArgs(
            "--input-start-ms is required".to_owned(),
        ));
    }
    if parsed.input_end_ms == 0 {
        return Err(BackfillError::InvalidArgs(
            "--input-end-ms is required".to_owned(),
        ));
    }
    if parsed.input_end_ms <= parsed.input_start_ms {
        return Err(BackfillError::InvalidArgs(
            "--input-end-ms must be greater than --input-start-ms".to_owned(),
        ));
    }
    if parsed.l0_s3_bucket.is_empty() {
        return Err(BackfillError::InvalidArgs(
            "--l0-s3-bucket is required".to_owned(),
        ));
    }
    if let Some(url) = parsed.rest_base_url.as_deref() {
        validate_https_url("--rest-base-url", url)?;
    }
    Ok(Some(parsed))
}

fn parse_venue(value: String) -> Result<Venue, BackfillError> {
    match value.as_str() {
        "binance" => Ok(Venue::Binance),
        "upbit" => Ok(Venue::Upbit),
        _ => Err(BackfillError::InvalidArgs(
            "--venue must be binance or upbit".to_owned(),
        )),
    }
}

fn parse_symbols(raw: &str) -> Result<Vec<String>, BackfillError> {
    let parsed = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if parsed.is_empty() {
        return Err(BackfillError::InvalidArgs(
            "--symbols requires at least one symbol".to_owned(),
        ));
    }
    Ok(parsed)
}

fn parse_positive_i64(value: String, name: &str) -> Result<i64, BackfillError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| BackfillError::InvalidArgs(format!("{name} must be a positive integer")))?;
    if parsed <= 0 {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be positive"
        )));
    }
    Ok(parsed)
}

fn parse_positive_usize(value: String, name: &str) -> Result<usize, BackfillError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| BackfillError::InvalidArgs(format!("{name} must be a positive integer")))?;
    if parsed == 0 {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be positive"
        )));
    }
    Ok(parsed)
}

fn parse_positive_u16(value: String, name: &str) -> Result<u16, BackfillError> {
    let parsed = value
        .parse::<u16>()
        .map_err(|_| BackfillError::InvalidArgs(format!("{name} must be a positive integer")))?;
    if parsed == 0 {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be positive"
        )));
    }
    Ok(parsed)
}

fn validate_https_url(name: &str, value: &str) -> Result<(), BackfillError> {
    if !value.starts_with("https://") {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must use https://"
        )));
    }
    Ok(())
}

pub fn print_help() {
    println!(
        "market-backfill\n\
         Usage:\n\
           cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml --bin market-backfill -- \\\n\
             --venue binance \\\n\
             --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \\\n\
             --input-start-ms 1778042400000 \\\n\
             --input-end-ms 1778043300000 \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214\n\
           cargo run --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml --bin market-backfill -- \\\n\
             --venue upbit \\\n\
             --input-start-ms 1778572800000 \\\n\
             --input-end-ms 1778573400000 \\\n\
             --symbols KRW-BTC,KRW-ETH \\\n\
             --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214\n\
         \n\
         This worker writes historical raw trade events into MARKET_L0_BUCKET only.\n\
         Binance uses public aggTrades for long-range trade backfill.\n\
         Upbit uses public recent trade history and rejects ranges older than the recent window."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Vec<String> {
        vec![
            "--venue".to_owned(),
            "binance".to_owned(),
            "--input-start-ms".to_owned(),
            "1".to_owned(),
            "--input-end-ms".to_owned(),
            "2".to_owned(),
            "--l0-s3-bucket".to_owned(),
            "bucket".to_owned(),
        ]
    }

    #[test]
    fn parses_symbols_as_uppercase() {
        let mut raw = base_args();
        raw.push("--symbols".to_owned());
        raw.push("btcusdt, ethusdt".to_owned());
        let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
        assert_eq!(
            parsed.symbols,
            Some(vec!["BTCUSDT".to_owned(), "ETHUSDT".to_owned()])
        );
    }

    #[test]
    fn rejects_missing_bucket() {
        let err = parse_args(
            vec![
                "--venue".to_owned(),
                "binance".to_owned(),
                "--input-start-ms".to_owned(),
                "1".to_owned(),
                "--input-end-ms".to_owned(),
                "2".to_owned(),
            ]
            .into_iter(),
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("--l0-s3-bucket"));
    }

    #[test]
    fn rejects_non_increasing_range() {
        let mut raw = base_args();
        raw[3] = "5".to_owned();
        raw[5] = "5".to_owned();
        let err = parse_args(raw.into_iter()).err().unwrap();
        assert!(err.to_string().contains("greater"));
    }
}
