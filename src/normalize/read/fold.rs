use super::super::args::{InputRange, NormalizeArgs};
use super::super::build::{BuildAccumulator, BuildResult};
use super::batch_decode::append_batches_to_accumulator;
use super::downloads::DownloadProgress;
use super::materialize::{materialize_entry, remove_file_best_effort};
use super::metadata::ReadInputMetadata;
use super::{DOWNLOAD_CONCURRENCY, L0InputReader};
use crate::storage::StorageError;
use futures_util::stream::{self, StreamExt};

impl L0InputReader {
    pub(super) async fn fold_range(
        &self,
        args: &NormalizeArgs,
        input_range: InputRange,
        scan_range: InputRange,
        read_range: InputRange,
    ) -> Result<BuildResult, StorageError> {
        let entries = self.input_entries(read_range).await?;
        let metadata = ReadInputMetadata::from_entries(&entries, self.run_mode);
        metadata.log_download_start();
        let input_s3_object_count = metadata.input_s3_object_count();
        let build_metadata = metadata.into_build_metadata(self.run_mode);
        let mut accumulator = BuildAccumulator::new(args, input_range, scan_range, build_metadata);

        let downloads = stream::iter(entries.into_iter().enumerate().map(|(index, entry)| {
            let s3 = self.s3.clone();
            let catchup_session_root = self.catchup_session_root.clone();
            async move { materialize_entry(s3, catchup_session_root, index, entry).await }
        }))
        .buffer_unordered(DOWNLOAD_CONCURRENCY);

        tokio::pin!(downloads);
        let mut download_progress = DownloadProgress::new(input_s3_object_count);
        while let Some(result) = downloads.next().await {
            let (_, materialized) = result?;
            download_progress.record(&materialized);
            append_batches_to_accumulator(
                &materialized.entry.key,
                &materialized.local_path,
                args,
                &mut accumulator,
            )?;
            if materialized.remove_after_read {
                remove_file_best_effort(&materialized.local_path).await;
            }
        }

        Ok(accumulator.finish())
    }
}
