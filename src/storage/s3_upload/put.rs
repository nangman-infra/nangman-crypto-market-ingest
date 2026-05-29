use super::S3Uploader;
use super::errors::{
    MAX_PUT_OBJECT_ATTEMPTS, PutObjectFailure, compact_error_message, is_not_found_head_error,
    retry_delay,
};
use crate::storage::StorageError;
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;

impl S3Uploader {
    pub async fn upload_file(&self, key: &str, path: &Path) -> Result<(), StorageError> {
        self.put_object_with_retry(
            key,
            tokio::fs::read(path).await?,
            "application/vnd.apache.parquet",
        )
        .await
    }

    pub async fn upload_json(&self, key: &str, bytes: Vec<u8>) -> Result<(), StorageError> {
        self.put_object_with_retry(key, bytes, "application/json")
            .await
    }

    pub async fn upload_json_if_pointer_current(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        let etag = self.head_etag(key).await?;
        self.put_object_with_retry_guarded(key, bytes, "application/json", etag)
            .await
    }

    async fn put_object_with_retry(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
    ) -> Result<(), StorageError> {
        self.put_object_with_retry_guarded(key, bytes, content_type, None)
            .await
    }

    async fn put_object_with_retry_guarded(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
        expected_etag: Option<String>,
    ) -> Result<(), StorageError> {
        for attempt in 1..=MAX_PUT_OBJECT_ATTEMPTS {
            let mut request = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(bytes.clone()))
                .content_type(content_type);
            request = match &expected_etag {
                Some(etag) => request.if_match(etag),
                None if content_type == "application/json" && key.starts_with("l1_index/") => {
                    request.if_none_match("*")
                }
                None => request,
            };
            let result = request.send().await;

            match result {
                Ok(_) => return Ok(()),
                Err(error) => {
                    let failure = PutObjectFailure::from_sdk_error(&self.bucket, key, error);
                    if attempt >= MAX_PUT_OBJECT_ATTEMPTS || !failure.retryable {
                        return Err(StorageError::Aws(failure.render(attempt)));
                    }
                    tokio::time::sleep(retry_delay(attempt, key)).await;
                }
            }
        }

        Err(StorageError::Aws(format!(
            "put_object bucket={} key={key} attempts={} code=unknown message=\"retry exhausted\"",
            self.bucket, MAX_PUT_OBJECT_ATTEMPTS
        )))
    }

    async fn head_etag(&self, key: &str) -> Result<Option<String>, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(output.e_tag().map(ToOwned::to_owned)),
            Err(error) if is_not_found_head_error(&error) => Ok(None),
            Err(error) => Err(StorageError::Aws(format!(
                "head_object bucket={} key={} error=\"{}\"",
                self.bucket,
                key,
                compact_error_message(&error.to_string())
            ))),
        }
    }
}
