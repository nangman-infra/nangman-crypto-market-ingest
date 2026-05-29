use super::super::super::Args;
use super::super::super::parse::{
    parse_pct, parse_positive_i64, parse_positive_u16, parse_positive_u64, parse_positive_usize,
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
        "--l0-s3-bucket" => {
            parsed.l0_s3_bucket = Some(required_arg(args, "--l0-s3-bucket requires a bucket")?);
        }
        "--aws-profile" => {
            parsed.aws_profile = Some(required_arg(args, "--aws-profile requires a profile")?);
        }
        "--aws-region" => {
            parsed.aws_region = required_arg(args, "--aws-region requires a region")?;
        }
        "--l0-spool-root" => {
            parsed.l0_spool_root = PathBuf::from(required_arg(
                args,
                "--l0-spool-root requires an absolute directory path",
            )?);
        }
        "--l0-flush-records" => {
            parsed.l0_flush_records = parse_positive_usize(
                required_arg(args, "--l0-flush-records requires a positive integer")?,
                "--l0-flush-records",
            )?;
        }
        "--l0-shard-count" => {
            parsed.l0_shard_count = parse_positive_u16(
                required_arg(args, "--l0-shard-count requires a positive integer")?,
                "--l0-shard-count",
            )?;
        }
        "--local-disk-high-water-pct" => {
            parsed.local_disk_high_water_pct = parse_pct(
                required_arg(args, "--local-disk-high-water-pct requires 1..100")?,
                "--local-disk-high-water-pct",
            )?;
        }
        "--local-disk-emergency-pct" => {
            parsed.local_disk_emergency_pct = parse_pct(
                required_arg(args, "--local-disk-emergency-pct requires 1..100")?,
                "--local-disk-emergency-pct",
            )?;
        }
        "--safety-floor-hours" => {
            parsed.safety_floor_hours = parse_positive_i64(
                required_arg(args, "--safety-floor-hours requires a positive integer")?,
                "--safety-floor-hours",
            )?;
        }
        "--eviction-check-interval-secs" => {
            parsed.eviction_check_interval_secs = parse_positive_u64(
                required_arg(
                    args,
                    "--eviction-check-interval-secs requires a positive integer",
                )?,
                "--eviction-check-interval-secs",
            )?;
        }
        _ => unreachable!("storage option dispatch mismatch: {arg}"),
    }
    Ok(())
}
