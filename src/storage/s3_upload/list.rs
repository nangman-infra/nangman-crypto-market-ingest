use super::errors::compact_error_message;
use super::{S3ObjectSummary, S3Uploader};
use crate::storage::StorageError;

impl S3Uploader {
    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let summaries = self.list_object_summaries(prefix).await?;
        Ok(summaries.into_iter().map(|summary| summary.key).collect())
    }

    pub async fn list_object_summaries(
        &self,
        prefix: &str,
    ) -> Result<Vec<S3ObjectSummary>, StorageError> {
        let mut objects = Vec::new();
        self.for_each_object_summary(prefix, |object| objects.push(object))
            .await?;
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }

    pub async fn for_each_object_summary<F>(
        &self,
        prefix: &str,
        mut visit: F,
    ) -> Result<(), StorageError>
    where
        F: FnMut(S3ObjectSummary),
    {
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            let output = request.send().await.map_err(|error| {
                StorageError::Aws(format!(
                    "list_objects_v2 bucket={} prefix={} error=\"{}\"",
                    self.bucket,
                    prefix,
                    compact_error_message(&error.to_string())
                ))
            })?;

            for object in output.contents() {
                if let Some(key) = object.key() {
                    visit(S3ObjectSummary {
                        key: key.to_owned(),
                        last_modified_ms: object
                            .last_modified()
                            .and_then(|last_modified| last_modified.to_millis().ok()),
                        size_bytes: object.size().unwrap_or(0).try_into().unwrap_or(0),
                    });
                }
            }

            if !output.is_truncated().unwrap_or(false) {
                break;
            }
            let Some(next_token) = output.next_continuation_token() else {
                break;
            };
            continuation_token = Some(next_token.to_owned());
        }

        Ok(())
    }

    pub async fn list_keys_page(
        &self,
        prefix: &str,
        max_keys: i32,
    ) -> Result<Vec<String>, StorageError> {
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_keys(max_keys)
            .send()
            .await
            .map_err(|error| {
                StorageError::Aws(format!(
                    "list_objects_v2 bucket={} prefix={} max_keys={} error=\"{}\"",
                    self.bucket,
                    prefix,
                    max_keys,
                    compact_error_message(&error.to_string())
                ))
            })?;
        let mut keys = output
            .contents()
            .iter()
            .filter_map(|object| object.key().map(ToOwned::to_owned))
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }
}
