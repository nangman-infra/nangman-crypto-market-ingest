use super::SchedulerError;
use super::SupervisorArgs;
use crate::clock;
use crate::normalize::write::index_pointer_key;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BootstrapChunk {
    pub(super) start_ms: i64,
    pub(super) end_ms: i64,
}

pub(super) struct BootstrapMarkerStore {
    uploader: S3Uploader,
}

impl BootstrapMarkerStore {
    pub(super) async fn new(args: &SupervisorArgs) -> Result<Self, SchedulerError> {
        let uploader = S3Uploader::new(
            args.l1_s3_bucket.clone(),
            args.aws_region.clone(),
            args.aws_profile.clone(),
        )
        .await?;
        Ok(Self { uploader })
    }

    pub(super) async fn has_complete(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<bool, SchedulerError> {
        let marker = self
            .uploader
            .download_json_optional::<serde_json::Value>(&self.complete_marker_key(chunk))
            .await?;
        Ok(marker.is_some())
    }

    pub(super) async fn has_l0_success(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<bool, SchedulerError> {
        let marker = self
            .uploader
            .download_json_optional::<serde_json::Value>(&self.l0_marker_key(chunk))
            .await?;
        Ok(marker.is_some())
    }

    pub(super) async fn has_l1_success(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<bool, SchedulerError> {
        let pointer_key = index_pointer_key(1_000, chunk.start_ms);
        let Some(pointer) = self
            .uploader
            .download_json_optional::<serde_json::Value>(&pointer_key)
            .await?
        else {
            return Ok(false);
        };
        Ok(pointer
            .get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status == "success")
            && pointer
                .get("input_time_range_start_ms")
                .and_then(serde_json::Value::as_i64)
                == Some(chunk.start_ms)
            && pointer
                .get("input_time_range_end_ms")
                .and_then(serde_json::Value::as_i64)
                == Some(chunk.end_ms))
    }

    pub(super) async fn mark_l0_success(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<(), SchedulerError> {
        let payload = serde_json::to_vec(&json!({
            "schema_version": "crypto_market_ingest_bootstrap_l0_marker_v1",
            "venue": "binance",
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "completed_at_ms": clock::now_ms()
        }))?;
        self.uploader
            .upload_json(&self.l0_marker_key(chunk), payload)
            .await?;
        Ok(())
    }

    pub(super) async fn mark_complete(&self, chunk: &BootstrapChunk) -> Result<(), SchedulerError> {
        let payload = serde_json::to_vec(&json!({
            "schema_version": "crypto_market_ingest_bootstrap_complete_marker_v1",
            "venue": "binance",
            "input_start_ms": chunk.start_ms,
            "input_end_ms": chunk.end_ms,
            "l0_marker_key": self.l0_marker_key(chunk),
            "completed_at_ms": clock::now_ms()
        }))?;
        self.uploader
            .upload_json(&self.complete_marker_key(chunk), payload)
            .await?;
        Ok(())
    }

    pub(super) fn l0_marker_key(&self, chunk: &BootstrapChunk) -> String {
        format!(
            "supervisor/bootstrap/venue=binance/start_ms={}/end_ms={}/success.json",
            chunk.start_ms, chunk.end_ms
        )
    }

    pub(super) fn complete_marker_key(&self, chunk: &BootstrapChunk) -> String {
        format!(
            "supervisor/bootstrap/venue=binance/start_ms={}/end_ms={}/complete.json",
            chunk.start_ms, chunk.end_ms
        )
    }
}

pub(super) fn bootstrap_chunks(args: &SupervisorArgs, now_ms: i64) -> Vec<BootstrapChunk> {
    let lookback_ms = args
        .bootstrap_lookback_days
        .saturating_mul(24)
        .saturating_mul(3_600_000);
    let chunk_ms = args.bootstrap_chunk_hours.saturating_mul(3_600_000);
    if chunk_ms <= 0 {
        return Vec::new();
    }
    let end_bound = align_down_to_chunk(now_ms.saturating_sub(3_600_000), chunk_ms);
    let start_bound = align_down_to_chunk(end_bound.saturating_sub(lookback_ms), chunk_ms);
    let mut chunks = Vec::new();
    let mut cursor = start_bound;
    while cursor < end_bound {
        let end_ms = cursor.saturating_add(chunk_ms).min(end_bound);
        if end_ms > cursor {
            chunks.push(BootstrapChunk {
                start_ms: cursor,
                end_ms,
            });
        }
        cursor = end_ms;
    }
    chunks
}

pub(super) fn normalize_subchunks(
    args: &SupervisorArgs,
    chunk: BootstrapChunk,
) -> Vec<BootstrapChunk> {
    let interval_ms = args.normalize_schedule_interval_ms;
    if interval_ms <= 0 || chunk.end_ms <= chunk.start_ms {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut cursor = chunk.start_ms;
    while cursor < chunk.end_ms {
        let end_ms = cursor.saturating_add(interval_ms).min(chunk.end_ms);
        if end_ms <= cursor {
            break;
        }
        chunks.push(BootstrapChunk {
            start_ms: cursor,
            end_ms,
        });
        cursor = end_ms;
    }
    chunks
}

pub(super) async fn next_missing_bootstrap_chunk(
    args: &SupervisorArgs,
    marker_store: &BootstrapMarkerStore,
) -> Result<Option<BootstrapChunk>, SchedulerError> {
    for chunk in bootstrap_chunks(args, clock::now_ms()) {
        if !marker_store.has_complete(&chunk).await? {
            return Ok(Some(chunk));
        }
    }
    Ok(None)
}

fn align_down_to_chunk(value: i64, chunk_ms: i64) -> i64 {
    value.div_euclid(chunk_ms) * chunk_ms
}
