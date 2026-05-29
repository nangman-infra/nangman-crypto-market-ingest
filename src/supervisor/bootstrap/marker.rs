use super::super::{SchedulerError, SupervisorArgs};
use super::chunk::BootstrapChunk;
use crate::clock;
use crate::normalize::write::index_pointer_key;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;

pub(in crate::supervisor) struct BootstrapMarkerStore {
    uploader: S3Uploader,
}

impl BootstrapMarkerStore {
    pub(in crate::supervisor) async fn new(args: &SupervisorArgs) -> Result<Self, SchedulerError> {
        let uploader = S3Uploader::new(
            args.l1_s3_bucket.clone(),
            args.aws_region.clone(),
            args.aws_profile.clone(),
        )
        .await?;
        Ok(Self { uploader })
    }

    pub(in crate::supervisor) async fn has_complete(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<bool, SchedulerError> {
        let marker = self
            .uploader
            .download_json_optional::<serde_json::Value>(&self.complete_marker_key(chunk))
            .await?;
        Ok(marker.is_some())
    }

    pub(in crate::supervisor) async fn has_l0_success(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<bool, SchedulerError> {
        let marker = self
            .uploader
            .download_json_optional::<serde_json::Value>(&self.l0_marker_key(chunk))
            .await?;
        Ok(marker.is_some())
    }

    pub(in crate::supervisor) async fn has_l1_success(
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

    pub(in crate::supervisor) async fn mark_l0_success(
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

    pub(in crate::supervisor) async fn mark_complete(
        &self,
        chunk: &BootstrapChunk,
    ) -> Result<(), SchedulerError> {
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

    pub(in crate::supervisor) fn l0_marker_key(&self, chunk: &BootstrapChunk) -> String {
        format!(
            "supervisor/bootstrap/venue=binance/start_ms={}/end_ms={}/success.json",
            chunk.start_ms, chunk.end_ms
        )
    }

    pub(in crate::supervisor) fn complete_marker_key(&self, chunk: &BootstrapChunk) -> String {
        format!(
            "supervisor/bootstrap/venue=binance/start_ms={}/end_ms={}/complete.json",
            chunk.start_ms, chunk.end_ms
        )
    }
}
