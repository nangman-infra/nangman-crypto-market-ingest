use crate::normalize::args::NormalizeArgs;
use crate::storage::{DualBucketRetention, S3RetentionLoopEvents, spawn_l0_l1_retention_loops};
use tokio::task::JoinHandle;

pub(super) fn spawn_s3_retention_loops(args: &NormalizeArgs) -> Vec<JoinHandle<()>> {
    if !args.s3_retention_enabled {
        return Vec::new();
    }
    spawn_l0_l1_retention_loops(DualBucketRetention {
        l0_bucket: args.l0_s3_bucket.clone(),
        l1_bucket: args.l1_s3_bucket.clone(),
        aws_region: args.aws_region.clone(),
        aws_profile: args.aws_profile.clone(),
        l0_retention_days: args.l0_s3_retention_days,
        l1_retention_days: args.l1_s3_retention_days,
        max_deletes_per_run: args.s3_retention_max_deletes_per_run,
        interval_secs: args.s3_retention_check_interval_secs,
        events: S3RetentionLoopEvents {
            run_event: "market_normalize_s3_retention_run",
            error_event: "market_normalize_s3_retention_error",
        },
    })
}
