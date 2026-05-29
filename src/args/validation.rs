use super::Args;
use std::error::Error;

pub(super) fn validate_and_normalize(parsed: &mut Args) -> Result<(), Box<dyn Error>> {
    if parsed.local_disk_emergency_pct < parsed.local_disk_high_water_pct {
        return Err("--local-disk-emergency-pct must be >= --local-disk-high-water-pct".into());
    }
    if let Some(bucket) = parsed.l0_s3_bucket.as_deref() {
        validate_real_bucket("--l0-s3-bucket", bucket)?;
    }
    if let Some(url) = parsed.live_nats_url.as_deref() {
        validate_nats_url("--live-nats-url", url)?;
        validate_non_empty_token("--live-nats-stream", &parsed.live_nats_stream)?;
        validate_subject_prefix(
            "--live-nats-subject-prefix",
            &parsed.live_nats_subject_prefix,
        )?;
    }

    if parsed.log_interval_seconds > parsed.duration_seconds {
        parsed.log_interval_seconds = parsed.duration_seconds;
    }
    Ok(())
}

fn validate_real_bucket(name: &str, value: &str) -> Result<(), Box<dyn Error>> {
    if value.trim().is_empty() {
        return Err(format!("{name} requires a bucket").into());
    }
    if value.contains('<') || value.contains('>') {
        return Err(
            format!("{name} must be a real bucket name, not a public-doc placeholder").into(),
        );
    }
    Ok(())
}

fn validate_nats_url(name: &str, value: &str) -> Result<(), Box<dyn Error>> {
    if !value.starts_with("nats://") {
        return Err(format!("{name} must start with nats://").into());
    }
    Ok(())
}

fn validate_non_empty_token(name: &str, value: &str) -> Result<(), Box<dyn Error>> {
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    if value.contains(char::is_whitespace) {
        return Err(format!("{name} must not contain whitespace").into());
    }
    Ok(())
}

fn validate_subject_prefix(name: &str, value: &str) -> Result<(), Box<dyn Error>> {
    validate_non_empty_token(name, value)?;
    if value.ends_with('.') {
        return Err(format!("{name} must not end with '.'").into());
    }
    Ok(())
}
