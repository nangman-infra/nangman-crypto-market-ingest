use super::super::{SchedulerError, SupervisorArgs};
use super::chunk::{BootstrapChunk, bootstrap_chunks};
use super::marker::BootstrapMarkerStore;
use crate::clock;

pub(in crate::supervisor) async fn next_missing_bootstrap_chunk(
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
