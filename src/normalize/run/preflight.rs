use crate::log_stream;
use crate::normalize::args::NormalizeArgs;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;
use std::error::Error;

pub(super) async fn run_preflight(args: &NormalizeArgs, now_ms: i64) -> Result<(), Box<dyn Error>> {
    preflight_bucket(args, "l0", &args.l0_s3_bucket, now_ms).await?;
    preflight_bucket(args, "l1", &args.l1_s3_bucket, now_ms).await?;
    log_stream::info(
        "market_normalize_preflight_ok",
        json!({
            "aws_profile": args.aws_profile.as_deref(),
            "aws_region": args.aws_region.as_str(),
            "l0_s3_bucket": args.l0_s3_bucket.as_str(),
            "l1_s3_bucket": args.l1_s3_bucket.as_str()
        }),
    )?;
    Ok(())
}

async fn preflight_bucket(
    args: &NormalizeArgs,
    label: &str,
    bucket: &str,
    now_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let uploader = S3Uploader::new(
        bucket.to_owned(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;
    let prefix = "_preflight/market-ingest-app/";
    uploader.list_keys(prefix).await?;
    let key = format!("{prefix}{label}-{now_ms}-{}.json", std::process::id());
    let bytes = serde_json::to_vec(&json!({
        "schema_version": "market_normalize_s3_preflight_v1",
        "label": label,
        "bucket": bucket,
        "timestamp_ms": now_ms
    }))?;
    uploader.upload_json(&key, bytes).await?;

    let tmp_path = args
        .catchup_tmp_root
        .join("_preflight")
        .join(format!("{label}-{now_ms}.json"));
    let download_result = uploader.download_file(&key, &tmp_path).await;
    let delete_result = uploader.delete_object(&key).await;
    let _ = std::fs::remove_file(&tmp_path);
    download_result?;
    delete_result?;
    Ok(())
}
