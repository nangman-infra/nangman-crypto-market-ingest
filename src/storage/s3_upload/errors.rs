use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use std::time::Duration;

pub(super) const MAX_PUT_OBJECT_ATTEMPTS: usize = 4;
const INITIAL_RETRY_DELAY_MS: u64 = 250;
pub(super) const MAX_RETRY_DELAY_MS: u64 = 2_000;

pub(super) fn is_not_found_get_error(error: &SdkError<GetObjectError>) -> bool {
    error
        .as_service_error()
        .and_then(|service_error| service_error.code())
        .is_some_and(is_not_found_code)
        || is_not_found_message(&error.to_string())
}

pub(super) fn is_not_found_head_error(error: &SdkError<HeadObjectError>) -> bool {
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
pub(super) struct PutObjectFailure {
    bucket: String,
    key: String,
    code: String,
    message: String,
    pub(super) retryable: bool,
}

impl PutObjectFailure {
    pub(super) fn from_sdk_error(bucket: &str, key: &str, error: SdkError<PutObjectError>) -> Self {
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

    pub(super) fn render(&self, attempts: usize) -> String {
        format!(
            "put_object bucket={} key={} attempts={} code={} message=\"{}\"",
            self.bucket, self.key, attempts, self.code, self.message
        )
    }
}

pub(super) fn retry_delay(attempt: usize, key: &str) -> Duration {
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

pub(super) fn is_retryable_put_object_code(code: &str) -> bool {
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

pub(super) fn compact_error_message(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}
