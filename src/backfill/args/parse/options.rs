use super::super::types::BackfillArgs;
use super::super::validation::{parse_positive_i64, parse_positive_u16, parse_positive_usize};
use super::values::{next_required_arg, parse_absolute_path, parse_symbols, parse_venue};
use crate::backfill::BackfillError;

pub(super) fn apply_arg(
    parsed: &mut BackfillArgs,
    arg: &str,
    args: &mut impl Iterator<Item = String>,
) -> Result<(), BackfillError> {
    match arg {
        "--venue" => {
            parsed.venue = parse_venue(next_required_arg(args, "--venue requires a value")?)?;
        }
        "--config" => {
            parsed.config_dir = parse_absolute_path(
                next_required_arg(args, "--config requires an absolute config directory path")?,
                "--config requires an absolute config directory path",
            )?;
        }
        "--rest-base-url" => {
            parsed.rest_base_url = Some(next_required_arg(
                args,
                "--rest-base-url requires an HTTPS URL",
            )?);
        }
        "--input-start-ms" => {
            parsed.input_start_ms = parse_positive_i64(
                next_required_arg(args, "--input-start-ms requires a positive integer")?,
                "--input-start-ms",
            )?;
        }
        "--input-end-ms" => {
            parsed.input_end_ms = parse_positive_i64(
                next_required_arg(args, "--input-end-ms requires a positive integer")?,
                "--input-end-ms",
            )?;
        }
        "--expect-symbol-count" => {
            parsed.expect_symbol_count = parse_positive_usize(
                next_required_arg(args, "--expect-symbol-count requires a positive integer")?,
                "--expect-symbol-count",
            )?;
        }
        "--symbols" => {
            let raw = next_required_arg(args, "--symbols requires comma-separated symbols")?;
            parsed.symbols = Some(parse_symbols(&raw)?);
        }
        "--upbit-quote-currency" => {
            parsed.upbit_quote_currency =
                next_required_arg(args, "--upbit-quote-currency requires a quote currency")?;
        }
        "--l0-s3-bucket" => {
            parsed.l0_s3_bucket = next_required_arg(args, "--l0-s3-bucket requires a bucket")?;
        }
        "--aws-profile" => {
            parsed.aws_profile = Some(next_required_arg(args, "--aws-profile requires a profile")?);
        }
        "--aws-region" => {
            parsed.aws_region = next_required_arg(args, "--aws-region requires a region")?;
        }
        "--l0-spool-root" => {
            parsed.l0_spool_root = parse_absolute_path(
                next_required_arg(args, "--l0-spool-root requires an absolute directory path")?,
                "--l0-spool-root requires an absolute directory path",
            )?;
        }
        "--l0-flush-records" => {
            parsed.l0_flush_records = parse_positive_usize(
                next_required_arg(args, "--l0-flush-records requires a positive integer")?,
                "--l0-flush-records",
            )?;
        }
        "--l0-shard-count" => {
            parsed.l0_shard_count = parse_positive_u16(
                next_required_arg(args, "--l0-shard-count requires a positive integer")?,
                "--l0-shard-count",
            )?;
        }
        "--disable-s3-retention" => {
            parsed.s3_retention_enabled = false;
        }
        "--s3-retention-days" => {
            parsed.s3_retention_days = parse_positive_i64(
                next_required_arg(args, "--s3-retention-days requires a positive integer")?,
                "--s3-retention-days",
            )?;
        }
        "--s3-retention-max-deletes-per-run" => {
            parsed.s3_retention_max_deletes_per_run = parse_positive_usize(
                next_required_arg(
                    args,
                    "--s3-retention-max-deletes-per-run requires a positive integer",
                )?,
                "--s3-retention-max-deletes-per-run",
            )?;
        }
        _ => {
            return Err(BackfillError::InvalidArgs(format!(
                "unknown argument: {arg}"
            )));
        }
    }
    Ok(())
}
