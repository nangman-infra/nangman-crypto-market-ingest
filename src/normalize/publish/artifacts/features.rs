use std::error::Error;

use serde_json::json;

use crate::log_stream;
use crate::normalize::args::InputRange;
use crate::normalize::build::BuildResult;
use crate::normalize::projection::{
    build_market_feature_delta_summary, build_market_feature_deltas, build_market_regime_contexts,
};
use crate::normalize::write::{
    market_feature_delta_object_key, market_feature_delta_summary_object_key,
    market_regime_context_object_key,
};
use crate::storage::s3_upload::S3Uploader;

use super::super::PublishedOutputKeys;

pub(in crate::normalize::publish) async fn publish_feature_deltas(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &BuildResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let feature_delta_key = market_feature_delta_object_key(l1_run_id);
    let feature_delta_summary_key = market_feature_delta_summary_object_key(l1_run_id);
    let feature_delta_count;
    let feature_delta_bytes;
    let feature_delta_summary_count;
    let feature_delta_summary_bytes;
    {
        let feature_deltas = build_market_feature_deltas(
            l1_run_id,
            input_range,
            finished_at_ms,
            &build.projection_slices,
            &build.projection_derivative_metrics,
        );
        feature_delta_count = feature_deltas.len();
        let feature_delta_summary = build_market_feature_delta_summary(
            l1_run_id,
            input_range,
            finished_at_ms,
            &feature_delta_key,
            &feature_deltas,
        );
        feature_delta_summary_count = feature_delta_summary.summary_row_count;
        feature_delta_summary_bytes = serde_json::to_vec(&feature_delta_summary)?;
        feature_delta_bytes = serde_json::to_vec(&feature_deltas)?;
        drop(feature_deltas);
    }
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_market_feature_delta_summary",
            "l1_run_id": l1_run_id,
            "key": &feature_delta_summary_key,
            "summary_row_count": feature_delta_summary_count,
            "detail_record_count": feature_delta_count,
            "bytes": feature_delta_summary_bytes.len()
        }),
    )?;
    uploader
        .upload_json(&feature_delta_summary_key, feature_delta_summary_bytes)
        .await?;
    published_keys.market_feature_delta_summary_key = Some(feature_delta_summary_key);
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_market_feature_delta",
            "l1_run_id": l1_run_id,
            "key": &feature_delta_key,
            "record_count": feature_delta_count,
            "bytes": feature_delta_bytes.len()
        }),
    )?;
    uploader
        .upload_json(&feature_delta_key, feature_delta_bytes)
        .await?;
    published_keys.market_feature_delta_key = Some(feature_delta_key);
    Ok(())
}

pub(in crate::normalize::publish) async fn publish_regime_contexts(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &mut BuildResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let regime_context_key = market_regime_context_object_key(l1_run_id);
    let regime_context_count;
    let regime_context_bytes;
    {
        let regime_contexts = build_market_regime_contexts(
            l1_run_id,
            input_range,
            finished_at_ms,
            &build.projection_slices,
        );
        regime_context_count = regime_contexts.len();
        regime_context_bytes = serde_json::to_vec(&regime_contexts)?;
        drop(regime_contexts);
    }
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_market_regime_context",
            "l1_run_id": l1_run_id,
            "key": &regime_context_key,
            "record_count": regime_context_count,
            "bytes": regime_context_bytes.len()
        }),
    )?;
    uploader
        .upload_json(&regime_context_key, regime_context_bytes)
        .await?;
    published_keys.market_regime_context_key = Some(regime_context_key);
    // projection inputs are large and no longer needed once delta+regime are published.
    build.projection_slices.clear();
    build.projection_slices.shrink_to_fit();
    build.projection_derivative_metrics.clear();
    build.projection_derivative_metrics.shrink_to_fit();
    Ok(())
}
