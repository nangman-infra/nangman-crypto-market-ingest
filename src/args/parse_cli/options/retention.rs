use super::super::super::Args;
use super::super::super::parse::{parse_positive_i64, parse_positive_u64, parse_positive_usize};
use super::super::required_arg;
use std::error::Error;

pub(super) fn apply_option(
    arg: &str,
    args: &mut impl Iterator<Item = String>,
    parsed: &mut Args,
) -> Result<(), Box<dyn Error>> {
    match arg {
        "--disable-s3-retention" => {
            parsed.s3_retention_enabled = false;
        }
        "--s3-retention-days" => {
            parsed.s3_retention_days = parse_positive_i64(
                required_arg(args, "--s3-retention-days requires a positive integer")?,
                "--s3-retention-days",
            )?;
        }
        "--s3-retention-check-interval-secs" => {
            parsed.s3_retention_check_interval_secs = parse_positive_u64(
                required_arg(
                    args,
                    "--s3-retention-check-interval-secs requires a positive integer",
                )?,
                "--s3-retention-check-interval-secs",
            )?;
        }
        "--s3-retention-max-deletes-per-run" => {
            parsed.s3_retention_max_deletes_per_run = parse_positive_usize(
                required_arg(
                    args,
                    "--s3-retention-max-deletes-per-run requires a positive integer",
                )?,
                "--s3-retention-max-deletes-per-run",
            )?;
        }
        _ => unreachable!("retention option dispatch mismatch: {arg}"),
    }
    Ok(())
}
