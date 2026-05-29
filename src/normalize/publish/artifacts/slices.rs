use std::error::Error;

use serde_json::json;

use crate::log_stream;
use crate::normalize::args::{InputRange, NormalizeArgs};
use crate::normalize::model::SliceRow;
use crate::normalize::projection::build_market_data_quality_summary;
use crate::normalize::write::{
    local_output_path, market_data_quality_summary_object_key, write_slice_parquet_refs,
};
use crate::storage::s3_upload::S3Uploader;

use super::super::{
    PublishedOutputKeys, file_size_best_effort, group_slices, remove_file_best_effort,
};

pub(in crate::normalize::publish) async fn publish_slice_parquets(
    uploader: &S3Uploader,
    args: &NormalizeArgs,
    l1_run_id: &str,
    slices: &[SliceRow],
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut output_keys = Vec::new();
    for (key, row_indices) in group_slices(l1_run_id, args.window_ms, slices) {
        let rows = row_indices
            .iter()
            .map(|index| &slices[*index])
            .collect::<Vec<_>>();
        let path = local_output_path(&args.spool_root, l1_run_id, &key);
        write_slice_parquet_refs(&path, &rows)?;
        let bytes = file_size_best_effort(&path).await.unwrap_or(0);
        log_stream::debug(
            "market_normalize_publishing",
            json!({
                "phase": "upload_parquet",
                "l1_run_id": l1_run_id,
                "key": key,
                "bytes": bytes,
                "row_count": rows.len()
            }),
        )?;
        uploader.upload_file(&key, &path).await?;
        output_keys.push(key);
        remove_file_best_effort(&path).await;
    }
    output_keys.sort();
    Ok(output_keys)
}

pub(in crate::normalize::publish) async fn publish_quality_summary(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    slices: &[SliceRow],
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let quality_key = market_data_quality_summary_object_key(l1_run_id);
    let quality_summary =
        build_market_data_quality_summary(l1_run_id, input_range, finished_at_ms, slices);
    let bytes = serde_json::to_vec(&quality_summary)?;
    uploader.upload_json(&quality_key, bytes).await?;
    published_keys.market_data_quality_summary_key = Some(quality_key);
    Ok(())
}
