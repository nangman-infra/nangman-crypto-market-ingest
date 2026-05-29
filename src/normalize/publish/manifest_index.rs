use super::{
    BuildResult, InputRange, NormalizeArgs, PublishedOutputKeys, RunTiming, manifest_object_key,
    report_object_key,
};
use crate::log_stream;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;
use std::error::Error;

mod index;
mod records;
mod verify;

#[cfg(test)]
mod tests;

use index::{publish_index_pointers, should_publish_index_pointers};
use records::{manifest, report};
use verify::verify_manifest;

pub(super) async fn publish_manifest_and_index(
    uploader: &S3Uploader,
    args: &NormalizeArgs,
    l1_run_id: &str,
    input_range: InputRange,
    build: BuildResult,
    timing: RunTiming,
    published_keys: PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let report_key = report_object_key(l1_run_id);
    let manifest_key = manifest_object_key(l1_run_id);
    let report = report(
        build,
        l1_run_id,
        input_range,
        timing,
        manifest_key.clone(),
        published_keys.clone(),
    );
    uploader
        .upload_json(&report_key, serde_json::to_vec_pretty(&report)?)
        .await?;

    let manifest = manifest(
        &report,
        l1_run_id,
        input_range,
        report_key,
        published_keys.clone(),
        timing,
    );
    uploader
        .upload_json(&manifest_key, serde_json::to_vec_pretty(&manifest)?)
        .await?;
    verify_manifest(uploader, &args.spool_root, l1_run_id, &manifest_key).await?;

    let index_pointer_count = if should_publish_index_pointers(report.status.as_str()) {
        publish_index_pointers(
            uploader,
            args,
            &manifest_key,
            l1_run_id,
            report.status.as_str(),
            timing.finished_at_ms,
            input_range,
        )
        .await?
    } else {
        0
    };

    log_stream::info(
        "market_normalize_index_published",
        json!({
            "l1_run_id": l1_run_id,
            "status": report.status,
            "window_ms": args.window_ms,
            "input_time_range_start_ms": input_range.start_ms,
            "input_time_range_end_ms": input_range.end_ms,
            "index_pointer_count": index_pointer_count
        }),
    )?;

    log_stream::info(
        "market_normalize_finished",
        json!({
            "l1_run_id": l1_run_id,
            "status": report.status,
            "slice_count_total": report.slice_count_total,
            "output_object_count": published_keys.slice_output_keys.len()
        }),
    )?;
    Ok(())
}
