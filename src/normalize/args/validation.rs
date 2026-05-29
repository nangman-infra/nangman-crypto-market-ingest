use super::NormalizeArgs;
use std::error::Error;

pub(super) fn validate_parsed_args(parsed: &NormalizeArgs) -> Result<(), Box<dyn Error>> {
    if parsed.l0_s3_bucket.is_empty() {
        return Err("--l0-s3-bucket is required".into());
    }
    if parsed.l1_s3_bucket.is_empty() {
        return Err("--l1-s3-bucket is required".into());
    }
    validate_real_bucket("--l0-s3-bucket", &parsed.l0_s3_bucket)?;
    validate_real_bucket("--l1-s3-bucket", &parsed.l1_s3_bucket)?;
    if parsed.watermark_delay_ms < parsed.scan_margin_ms {
        return Err("--watermark-delay-ms must be >= --scan-margin-ms".into());
    }
    if (parsed.input_start_ms.is_some()) != (parsed.input_end_ms.is_some()) {
        return Err("--input-start-ms and --input-end-ms must be provided together".into());
    }
    if (parsed.audit_l1_index_start_ms.is_some()) != (parsed.audit_l1_index_end_ms.is_some()) {
        return Err(
            "--audit-l1-index-start-ms and --audit-l1-index-end-ms must be provided together"
                .into(),
        );
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
