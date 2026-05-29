use super::errors::{
    MAX_RETRY_DELAY_MS, compact_error_message, is_retryable_put_object_code, retry_delay,
};
use std::time::Duration;

#[test]
fn compacts_multiline_aws_error_messages() {
    let message =
        "User is not authorized\n  to perform: s3:PutObject\t on resource arn:aws:s3:::bucket/key";

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
