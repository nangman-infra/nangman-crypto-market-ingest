use super::S3Uploader;
use super::errors::{compact_error_message, is_not_found_get_error};
use crate::storage::StorageError;
use serde::de::DeserializeOwned;
use std::path::Path;

impl S3Uploader {
    pub async fn download_file(&self, key: &str, path: &Path) -> Result<(), StorageError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                StorageError::Aws(format!(
                    "get_object bucket={} key={} error=\"{}\"",
                    self.bucket,
                    key,
                    compact_error_message(&error.to_string())
                ))
            })?;
        let bytes = output.body.collect().await.map_err(|error| {
            StorageError::Aws(format!(
                "get_object_body bucket={} key={} error=\"{}\"",
                self.bucket,
                key,
                compact_error_message(&error.to_string())
            ))
        })?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, bytes.into_bytes()).await?;
        Ok(())
    }

    pub async fn download_json_optional<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StorageError> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if is_not_found_get_error(&error) => return Ok(None),
            Err(error) => {
                return Err(StorageError::Aws(format!(
                    "get_object bucket={} key={} error=\"{}\"",
                    self.bucket,
                    key,
                    compact_error_message(&error.to_string())
                )));
            }
        };
        let bytes = output.body.collect().await.map_err(|error| {
            StorageError::Aws(format!(
                "get_object_body bucket={} key={} error=\"{}\"",
                self.bucket,
                key,
                compact_error_message(&error.to_string())
            ))
        })?;
        Ok(Some(serde_json::from_slice(&bytes.into_bytes())?))
    }

    pub async fn delete_object(&self, key: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                StorageError::Aws(format!(
                    "delete_object bucket={} key={} error=\"{}\"",
                    self.bucket,
                    key,
                    compact_error_message(&error.to_string())
                ))
            })?;
        Ok(())
    }
}
