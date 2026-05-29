use super::args::{InputRange, NormalizeArgs};
use super::build::BuildResult;
use super::input_keys::{InputEntry, collect_input_entries};
use super::mode::RunMode;
use super::model::NormalizeInputs;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use std::path::{Path, PathBuf};

mod batch_decode;
mod downloads;
mod fold;
mod materialize;
mod metadata;
mod range;
#[cfg(test)]
mod tests;

const DOWNLOAD_CONCURRENCY: usize = 16;

pub async fn read_inputs(
    args: &NormalizeArgs,
    range: InputRange,
    run_mode: RunMode,
    session_id: &str,
) -> Result<NormalizeInputs, StorageError> {
    let reader = L0InputReader::new(args, run_mode, session_id).await?;
    reader.read_range(range).await
}

pub async fn read_and_build_slices(
    args: &NormalizeArgs,
    input_range: InputRange,
    scan_range: InputRange,
    read_range: InputRange,
    run_mode: RunMode,
    session_id: &str,
) -> Result<BuildResult, StorageError> {
    let reader = L0InputReader::new(args, run_mode, session_id).await?;
    reader
        .fold_range(args, input_range, scan_range, read_range)
        .await
}

/// Best-effort cleanup of the catch-up tmp directory for a run. Called by
/// the orchestrator after publish succeeds (or fails) to enforce the
/// `catchup_tmp_lifecycle` invariant: the session dir must not survive the run.
pub async fn cleanup_session_tmp(catchup_tmp_root: &Path, session_id: &str) {
    let session_dir = catchup_tmp_root.join(session_id);
    let _ = tokio::fs::remove_dir_all(&session_dir).await;
}

struct L0InputReader {
    s3: S3Uploader,
    l0_local_root: PathBuf,
    catchup_session_root: PathBuf,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
}

impl L0InputReader {
    async fn new(
        args: &NormalizeArgs,
        run_mode: RunMode,
        session_id: &str,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            s3: S3Uploader::new(
                args.l0_s3_bucket.clone(),
                args.aws_region.clone(),
                args.aws_profile.clone(),
            )
            .await?,
            l0_local_root: args.l0_local_root.clone(),
            catchup_session_root: args.catchup_tmp_root.join(session_id),
            run_mode,
            l0_run_key_overlap_ms: args.l0_run_key_overlap_ms,
        })
    }

    async fn input_entries(&self, range: InputRange) -> Result<Vec<InputEntry>, StorageError> {
        collect_input_entries(
            &self.s3,
            &self.l0_local_root,
            range,
            self.run_mode,
            self.l0_run_key_overlap_ms,
        )
        .await
    }
}
