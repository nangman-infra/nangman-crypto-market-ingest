use super::time::hourly_parts;
use crate::log_stream;
use crate::normalize::args::InputRange;
use crate::normalize::mode::RunMode;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;
use std::collections::BTreeSet;

mod prefixes;
mod selection;

pub(super) use selection::select_s3_keys;

pub(super) async fn s3_input_keys(
    s3: &S3Uploader,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Result<Vec<String>, StorageError> {
    let mut keys = BTreeSet::new();
    for part in hourly_parts(range.start_ms, range.end_ms) {
        for prefix in prefixes::input_prefixes_for_part(&part) {
            list_into(
                s3,
                &prefix,
                &mut keys,
                range,
                run_mode,
                l0_run_key_overlap_ms,
            )
            .await?;
        }
    }

    Ok(keys.into_iter().collect())
}

async fn list_into(
    s3: &S3Uploader,
    prefix: &str,
    keys: &mut BTreeSet<String>,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Result<(), StorageError> {
    let _ = log_stream::debug(
        "market_normalize_listing_inputs",
        json!({
            "phase": "start",
            "prefix": prefix
        }),
    );
    let listed_keys = s3.list_keys(prefix).await?;
    let listed_key_count = listed_keys.len();
    let parquet_key_count = listed_keys
        .iter()
        .filter(|key| key.ends_with(".parquet"))
        .count();
    let selected_keys = select_s3_keys(listed_keys, range, run_mode, l0_run_key_overlap_ms);
    let selected_key_count = selected_keys.len();
    keys.extend(selected_keys);
    let _ = log_stream::debug(
        "market_normalize_listing_inputs",
        json!({
            "phase": "finished",
            "prefix": prefix,
            "listed_key_count": listed_key_count,
            "parquet_key_count": parquet_key_count,
            "selected_key_count": selected_key_count
        }),
    );
    Ok(())
}
