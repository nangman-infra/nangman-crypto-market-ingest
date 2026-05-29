use super::super::args::NormalizeArgs;
use super::json_doc::{
    download_json_value, input_time_range_end_ms, json_status_success, pointer_manifest_key,
};
use super::time::{HOUR_MS, floor_hour_ms, time_part};
use crate::clock;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use std::path::Path;

const MAX_L1_POINTER_LOOKBACK_HOURS: i64 = 24 * 90;
const L1_POINTER_KEYS_PER_HOUR: i32 = 1_000;

/// Best-effort lookup for the most recent successful L1 manifest's
/// `input_time_range_end_ms`. Returns Ok(None) if no success exists yet.
pub(in crate::normalize) async fn resolve_last_l1_success_end_ms(
    args: &NormalizeArgs,
) -> Result<Option<i64>, StorageError> {
    let uploader = S3Uploader::new(
        args.l1_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;

    let latest_hour_start_ms = floor_hour_ms(clock::now_ms());
    for offset in 0..MAX_L1_POINTER_LOOKBACK_HOURS {
        let hour_start_ms = latest_hour_start_ms.saturating_sub(offset.saturating_mul(HOUR_MS));
        let prefix = l1_pointer_hour_prefix(args.window_ms, hour_start_ms);
        let pointer_keys = json_keys_desc(
            uploader
                .list_keys_page(&prefix, L1_POINTER_KEYS_PER_HOUR)
                .await?,
        );

        for pointer_key in pointer_keys {
            if let Some(end_ms) =
                success_end_ms_from_pointer(&uploader, &args.catchup_tmp_root, &pointer_key).await?
            {
                return Ok(Some(end_ms));
            }
        }
    }

    Ok(None)
}

async fn success_end_ms_from_pointer(
    uploader: &S3Uploader,
    tmp_root: &Path,
    pointer_key: &str,
) -> Result<Option<i64>, StorageError> {
    let pointer = download_json_value(uploader, tmp_root, pointer_key, "pointer").await?;
    if !json_status_success(&pointer) {
        return Ok(None);
    }

    if let Some(end_ms) = input_time_range_end_ms(&pointer) {
        return Ok(Some(end_ms));
    }

    let Some(manifest_key) = pointer_manifest_key(&pointer) else {
        return Ok(None);
    };

    let manifest = download_json_value(uploader, tmp_root, manifest_key, "manifest").await?;
    if !json_status_success(&manifest) {
        return Ok(None);
    }

    Ok(input_time_range_end_ms(&manifest))
}

pub(super) fn l1_pointer_hour_prefix(window_ms: i64, hour_start_ms: i64) -> String {
    let part = time_part(hour_start_ms);
    format!(
        "l1_index/window_ms={window_ms}/event_date={}/hour={:02}/",
        part.event_date, part.hour
    )
}

pub(super) fn json_keys_desc(mut keys: Vec<String>) -> Vec<String> {
    keys.retain(|key| key.ends_with(".json"));
    keys.sort_by(|left, right| right.cmp(left));
    keys
}
