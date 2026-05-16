use super::StorageError;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use serde::de::DeserializeOwned;
use std::env;
use std::path::Path;
use std::time::Duration;

const MAX_PUT_OBJECT_ATTEMPTS: usize = 4;
const INITIAL_RETRY_DELAY_MS: u64 = 250;
const MAX_RETRY_DELAY_MS: u64 = 2_000;

#[derive(Debug, Clone)]
pub struct S3Uploader {
    client: Client,
    bucket: String,
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
        if let Some(endpoint) = env_s3_endpoint() {
            config_loader = config_loader.endpoint_url(endpoint);
        }
        let config = config_loader.load().await;
        let s3_config = S3ConfigBuilder::from(&config)
            .force_path_style(
                env_bool("AWS_S3_FORCE_PATH_STYLE") || env_bool("AWS_USE_PATH_STYLE_ENDPOINT"),
            )
            .build();
        Ok(Self {
            client: Client::from_conf(s3_config),
            bucket,
        })
    }

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

    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let summaries = self.list_object_summaries(prefix).await?;
        Ok(summaries.into_iter().map(|summary| summary.key).collect())
    }

    pub async fn list_object_summaries(
        &self,
        prefix: &str,
    ) -> Result<Vec<S3ObjectSummary>, StorageError> {
        let mut objects = Vec::new();
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
                    objects.push(S3ObjectSummary {
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

        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
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

fn is_not_found_get_error(error: &SdkError<GetObjectError>) -> bool {
    error
        .as_service_error()
        .and_then(|service_error| service_error.code())
        .is_some_and(is_not_found_code)
        || is_not_found_message(&error.to_string())
}

fn is_not_found_head_error(error: &SdkError<HeadObjectError>) -> bool {
    error
        .as_service_error()
        .and_then(|service_error| service_error.code())
        .is_some_and(is_not_found_code)
        || is_not_found_message(&error.to_string())
}

fn is_not_found_code(code: &str) -> bool {
    matches!(code, "404" | "NoSuchKey" | "NotFound")
}

fn is_not_found_message(message: &str) -> bool {
    message.contains("NoSuchKey") || message.contains("NotFound") || message.contains("404")
}

#[derive(Debug, Clone)]
struct PutObjectFailure {
    bucket: String,
    key: String,
    code: String,
    message: String,
    retryable: bool,
}

impl PutObjectFailure {
    fn from_sdk_error(bucket: &str, key: &str, error: SdkError<PutObjectError>) -> Self {
        let service_error = error.into_service_error();
        let code = service_error
            .code()
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown")
            .to_owned();
        let message = service_error
            .message()
            .map(compact_error_message)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| compact_error_message(&service_error.to_string()));
        let retryable = is_retryable_put_object_code(&code);

        Self {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            code,
            message,
            retryable,
        }
    }

    fn render(&self, attempts: usize) -> String {
        format!(
            "put_object bucket={} key={} attempts={} code={} message=\"{}\"",
            self.bucket, self.key, attempts, self.code, self.message
        )
    }
}

fn retry_delay(attempt: usize, key: &str) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let backoff_ms = INITIAL_RETRY_DELAY_MS
        .saturating_mul(2_u64.saturating_pow(exponent))
        .min(MAX_RETRY_DELAY_MS);
    let jitter_ms = stable_jitter_ms(key);
    Duration::from_millis(backoff_ms.saturating_add(jitter_ms))
}

fn stable_jitter_ms(key: &str) -> u64 {
    key.as_bytes()
        .iter()
        .fold(0_u64, |acc, value| acc.wrapping_add(u64::from(*value)))
        % 100
}

fn is_retryable_put_object_code(code: &str) -> bool {
    matches!(
        code,
        "unknown"
            | "InternalError"
            | "RequestTimeout"
            | "ServiceUnavailable"
            | "SlowDown"
            | "Throttling"
            | "ThrottlingException"
            | "TooManyRequestsException"
    ) || code.starts_with('5')
}

fn compact_error_message(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn env_s3_endpoint() -> Option<String> {
    env::var("AWS_ENDPOINT_URL_S3")
        .ok()
        .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
        .map(|value| value.trim().trim_end_matches('/').to_owned())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_RETRY_DELAY_MS, compact_error_message, is_retryable_put_object_code, retry_delay,
    };
    use std::time::Duration;

    #[test]
    fn compacts_multiline_aws_error_messages() {
        let message = "User is not authorized\n  to perform: s3:PutObject\t on resource arn:aws:s3:::bucket/key";

        assert_eq!(
            compact_error_message(message),
            "User is not authorized to perform: s3:PutObject on resource arn:aws:s3:::bucket/key"
        );
    }

    #[test]
    fn classifies_retryable_and_terminal_s3_errors() {
        assert!(is_retryable_put_object_code("SlowDown"));
        assert!(is_retryable_put_object_code("ServiceUnavailable"));
        assert!(is_retryable_put_object_code("500"));
        assert!(!is_retryable_put_object_code("AccessDenied"));
        assert!(!is_retryable_put_object_code("SignatureDoesNotMatch"));
    }

    #[test]
    fn retry_delay_uses_bounded_backoff_with_stable_jitter() {
        let first = retry_delay(1, "raw/file.parquet");
        let second = retry_delay(2, "raw/file.parquet");
        let late = retry_delay(99, "raw/file.parquet");

        assert!(second > first);
        assert!(late <= Duration::from_millis(MAX_RETRY_DELAY_MS + 99));
        assert_eq!(retry_delay(2, "raw/file.parquet"), second);
    }
}
