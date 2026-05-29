use super::types::BackfillArgs;
use crate::backfill::BackfillError;

pub(super) fn validate_completed_args(parsed: &BackfillArgs) -> Result<(), BackfillError> {
    if parsed.input_start_ms == 0 {
        return Err(BackfillError::InvalidArgs(
            "--input-start-ms is required".to_owned(),
        ));
    }
    if parsed.input_end_ms == 0 {
        return Err(BackfillError::InvalidArgs(
            "--input-end-ms is required".to_owned(),
        ));
    }
    if parsed.input_end_ms <= parsed.input_start_ms {
        return Err(BackfillError::InvalidArgs(
            "--input-end-ms must be greater than --input-start-ms".to_owned(),
        ));
    }
    if parsed.l0_s3_bucket.is_empty() {
        return Err(BackfillError::InvalidArgs(
            "--l0-s3-bucket is required".to_owned(),
        ));
    }
    validate_real_bucket("--l0-s3-bucket", &parsed.l0_s3_bucket)?;
    if let Some(url) = parsed.rest_base_url.as_deref() {
        validate_https_url("--rest-base-url", url)?;
    }
    Ok(())
}

pub(super) fn parse_positive_i64(value: String, name: &str) -> Result<i64, BackfillError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| BackfillError::InvalidArgs(format!("{name} must be a positive integer")))?;
    if parsed <= 0 {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be positive"
        )));
    }
    Ok(parsed)
}

pub(super) fn parse_positive_usize(value: String, name: &str) -> Result<usize, BackfillError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| BackfillError::InvalidArgs(format!("{name} must be a positive integer")))?;
    if parsed == 0 {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be positive"
        )));
    }
    Ok(parsed)
}

pub(super) fn parse_positive_u16(value: String, name: &str) -> Result<u16, BackfillError> {
    let parsed = value
        .parse::<u16>()
        .map_err(|_| BackfillError::InvalidArgs(format!("{name} must be a positive integer")))?;
    if parsed == 0 {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be positive"
        )));
    }
    Ok(parsed)
}

fn validate_https_url(name: &str, value: &str) -> Result<(), BackfillError> {
    let url = reqwest::Url::parse(value.trim())
        .map_err(|error| BackfillError::InvalidArgs(format!("{name} must be a URL: {error}")))?;
    if url.scheme() != "https" {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must use https://"
        )));
    }
    if url.host_str().is_none() {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must include a host"
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must not include credentials"
        )));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must not include query or fragment components"
        )));
    }
    Ok(())
}

fn validate_real_bucket(name: &str, value: &str) -> Result<(), BackfillError> {
    if value.trim().is_empty() {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} requires a bucket"
        )));
    }
    if value.contains('<') || value.contains('>') {
        return Err(BackfillError::InvalidArgs(format!(
            "{name} must be a real bucket name, not a public-doc placeholder"
        )));
    }
    Ok(())
}
