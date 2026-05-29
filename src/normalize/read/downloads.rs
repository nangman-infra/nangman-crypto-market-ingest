use super::materialize::{MaterializedInputEntry, materialize_entry};
use super::{DOWNLOAD_CONCURRENCY, InputEntry, L0InputReader};
use crate::log_stream;
use crate::storage::StorageError;
use futures_util::stream::{self, StreamExt};
use serde_json::json;
use tokio::time::{Duration, Instant};

impl L0InputReader {
    pub(super) async fn materialize_entries(
        &self,
        entries: Vec<InputEntry>,
        input_s3_object_count: usize,
    ) -> Result<Vec<MaterializedInputEntry>, StorageError> {
        let mut materialized_entries = Vec::with_capacity(entries.len());
        for _ in 0..entries.len() {
            materialized_entries.push(None);
        }

        let downloads = stream::iter(entries.into_iter().enumerate().map(|(index, entry)| {
            let s3 = self.s3.clone();
            let catchup_session_root = self.catchup_session_root.clone();
            async move { materialize_entry(s3, catchup_session_root, index, entry).await }
        }))
        .buffer_unordered(DOWNLOAD_CONCURRENCY);

        tokio::pin!(downloads);
        let mut download_progress = DownloadProgress::new(input_s3_object_count);
        while let Some(result) = downloads.next().await {
            let (index, materialized) = result?;
            download_progress.record(&materialized);
            materialized_entries[index] = Some(materialized);
        }

        materialized_entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                entry.ok_or_else(|| {
                    StorageError::InvalidConfig(format!(
                        "normalize reader materialization missing entry at index {index}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

pub(super) struct DownloadProgress {
    input_s3_object_count: usize,
    downloaded_s3_object_count: usize,
    downloaded_s3_bytes: u64,
    next_download_progress_at: Instant,
}

impl DownloadProgress {
    pub(super) fn new(input_s3_object_count: usize) -> Self {
        Self {
            input_s3_object_count,
            downloaded_s3_object_count: 0,
            downloaded_s3_bytes: 0,
            next_download_progress_at: Instant::now() + Duration::from_secs(10),
        }
    }

    pub(super) fn record(&mut self, materialized: &MaterializedInputEntry) {
        if !materialized.remove_after_read {
            return;
        }

        self.downloaded_s3_object_count += 1;
        self.downloaded_s3_bytes = self
            .downloaded_s3_bytes
            .saturating_add(materialized.downloaded_bytes);
        if self.should_log() {
            let _ = log_stream::debug(
                "market_normalize_download_progress",
                json!({
                    "downloaded_files": self.downloaded_s3_object_count,
                    "total_files": self.input_s3_object_count,
                    "downloaded_bytes": self.downloaded_s3_bytes,
                    "download_concurrency": DOWNLOAD_CONCURRENCY
                }),
            );
            self.next_download_progress_at = Instant::now() + Duration::from_secs(10);
        }
    }

    fn should_log(&self) -> bool {
        self.downloaded_s3_object_count == self.input_s3_object_count
            || self.downloaded_s3_object_count.is_multiple_of(10)
            || Instant::now() >= self.next_download_progress_at
    }
}
