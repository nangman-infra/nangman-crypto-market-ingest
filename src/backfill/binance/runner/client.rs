use crate::backfill::BackfillError;
use std::time::Duration;

pub(super) fn build_client() -> Result<reqwest::Client, BackfillError> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?)
}
