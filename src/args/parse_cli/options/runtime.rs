use super::super::super::Args;
use super::super::super::parse::{
    parse_depth_snapshot_limit, parse_positive_u64, parse_positive_usize, parse_venue,
};
use super::super::required_arg;
use std::error::Error;
use std::path::PathBuf;

pub(super) fn apply_option(
    arg: &str,
    args: &mut impl Iterator<Item = String>,
    parsed: &mut Args,
) -> Result<(), Box<dyn Error>> {
    match arg {
        "--venue" => {
            parsed.venue = parse_venue(required_arg(args, "--venue requires binance or upbit")?)?;
        }
        "--config" => {
            parsed.config_dir = PathBuf::from(required_arg(
                args,
                "--config requires an absolute config directory path",
            )?);
        }
        "--duration-seconds" => {
            parsed.duration_seconds = parse_positive_u64(
                required_arg(args, "--duration-seconds requires a positive integer")?,
                "--duration-seconds",
            )?;
        }
        "--log-interval-seconds" => {
            parsed.log_interval_seconds = parse_positive_u64(
                required_arg(args, "--log-interval-seconds requires a positive integer")?,
                "--log-interval-seconds",
            )?;
        }
        "--depth-snapshot-limit" => {
            parsed.depth_snapshot_limit = parse_depth_snapshot_limit(args.next())?;
        }
        "--expect-symbol-count" => {
            parsed.expect_symbol_count = parse_positive_usize(
                required_arg(args, "--expect-symbol-count requires a positive integer")?,
                "--expect-symbol-count",
            )?;
        }
        "--allow-partial-symbol-coverage" => {
            parsed.allow_partial_symbol_coverage = true;
        }
        _ => unreachable!("runtime option dispatch mismatch: {arg}"),
    }
    Ok(())
}
