use super::super::args::InputRange;
use super::super::model::NormalizeInputs;
use super::L0InputReader;
use super::batch_decode::append_batches;
use super::materialize::remove_file_best_effort;
use super::metadata::ReadInputMetadata;
use crate::storage::StorageError;

impl L0InputReader {
    pub(super) async fn read_range(
        &self,
        range: InputRange,
    ) -> Result<NormalizeInputs, StorageError> {
        let entries = self.input_entries(range).await?;
        let metadata = ReadInputMetadata::from_entries(&entries, self.run_mode);
        metadata.log_download_start();
        let input_s3_object_count = metadata.input_s3_object_count();
        let mut inputs = metadata.into_normalize_inputs(self.run_mode);

        let materialized_entries = self
            .materialize_entries(entries, input_s3_object_count)
            .await?;
        for materialized in materialized_entries {
            append_batches(
                &materialized.entry.key,
                &materialized.local_path,
                &mut inputs,
            )?;
            if materialized.remove_after_read {
                remove_file_best_effort(&materialized.local_path).await;
            }
        }

        Ok(inputs)
    }
}
