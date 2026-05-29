use super::super::args::NormalizeArgs;
use super::super::input_keys::{RAW_EVENT_TYPES, VENUES};
use super::time::parse_event_date_hour_ms;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;

const L0_BOOTSTRAP_KEYS_PER_PREFIX: i32 = 1;

/// Best-effort lookup for the oldest L0 raw_market_event object's start-of-hour
/// timestamp (UTC ms). Used only when no L1 success history exists.
pub(in crate::normalize) async fn resolve_oldest_l0_object_ms(
    args: &NormalizeArgs,
) -> Result<Option<i64>, StorageError> {
    let uploader = S3Uploader::new(
        args.l0_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;

    let mut oldest: Option<i64> = None;
    for venue in VENUES {
        for event_type in RAW_EVENT_TYPES {
            let prefix = format!("raw_market_event/venue={venue}/event_type={event_type}/");
            let keys = uploader
                .list_keys_page(&prefix, L0_BOOTSTRAP_KEYS_PER_PREFIX)
                .await?;
            let Some(key) = keys.into_iter().find(|key| key.ends_with(".parquet")) else {
                continue;
            };
            let Some(candidate) = parse_event_date_hour_ms(&key) else {
                continue;
            };
            oldest = Some(oldest.map_or(candidate, |current| current.min(candidate)));
        }
    }

    Ok(oldest)
}
