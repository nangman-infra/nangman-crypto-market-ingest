mod download;
mod errors;
mod list;
mod put;
#[cfg(test)]
mod tests;

use super::StorageError;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_types::region::Region;

#[derive(Debug, Clone)]
pub struct S3Uploader {
    pub(super) client: Client,
    pub(super) bucket: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectSummary {
    pub key: String,
    pub last_modified_ms: Option<i64>,
    pub size_bytes: u64,
}

impl S3Uploader {
    pub async fn new(
        bucket: String,
        region: String,
        profile: Option<String>,
    ) -> Result<Self, StorageError> {
        let mut config_loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));
        if let Some(profile) = profile {
            config_loader = config_loader.profile_name(profile);
        }
        let config = config_loader.load().await;
        Ok(Self {
            client: Client::new(&config),
            bucket,
        })
    }
}
