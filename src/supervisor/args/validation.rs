use super::types::SupervisorArgs;
use std::error::Error;

pub(super) fn next_arg(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, Box<dyn Error>> {
    args.next()
        .ok_or_else(|| format!("{name} requires a value").into())
}

pub(super) fn validate_completed_args(parsed: &mut SupervisorArgs) -> Result<(), Box<dyn Error>> {
    if parsed.realtime_venues.is_empty() {
        return Err("--realtime-venues requires at least one venue".into());
    }
    for venue in &parsed.realtime_venues {
        validate_venue(venue, "--realtime-venues")?;
    }
    parsed.realtime_venue = parsed.realtime_venues[0].clone();
    validate_bucket_arg(&parsed.l0_s3_bucket, "--l0-s3-bucket")?;
    validate_bucket_arg(&parsed.l1_s3_bucket, "--l1-s3-bucket")?;
    if let Some(url) = parsed.live_nats_url.as_deref() {
        validate_nats_url("--live-nats-url", url)?;
        validate_non_empty_token("--live-nats-stream", &parsed.live_nats_stream)?;
        validate_subject_prefix(
            "--live-nats-subject-prefix",
            &parsed.live_nats_subject_prefix,
        )?;
    }
    if parsed.bootstrap_lookback_days > 0 && parsed.bootstrap_chunk_hours > 24 {
        return Err("--bootstrap-chunk-hours must be <= 24 to keep recovery chunks bounded".into());
    }
    if 24 % parsed.bootstrap_chunk_hours != 0 {
        return Err(
            "--bootstrap-chunk-hours must evenly divide 24 for stable UTC day partitions".into(),
        );
    }
    Ok(())
}

fn validate_bucket_arg(value: &str, name: &str) -> Result<(), Box<dyn Error>> {
    if value.trim().is_empty() {
        return Err(format!("{name} is required").into());
    }
    if value.contains('<') || value.contains('>') {
        return Err(
            format!("{name} must be a real bucket name, not a public-doc placeholder").into(),
        );
    }
    Ok(())
}

pub(super) fn parse_positive_i64(value: String) -> Result<i64, Box<dyn Error>> {
    let parsed = value.parse::<i64>()?;
    if parsed <= 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_u64(value: String) -> Result<u64, Box<dyn Error>> {
    let parsed = value.parse::<u64>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_usize(value: String) -> Result<usize, Box<dyn Error>> {
    let parsed = value.parse::<usize>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_u16(value: String) -> Result<u16, Box<dyn Error>> {
    let parsed = value.parse::<u16>()?;
    if parsed == 0 {
        return Err("value must be positive".into());
    }
    Ok(parsed)
}

pub(super) fn parse_symbols(value: &str, name: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let symbols = value
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(format!("{name} requires at least one symbol").into());
    }
    Ok(symbols)
}

pub(super) fn parse_venues(value: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let venues = value
        .split(',')
        .map(str::trim)
        .filter(|venue| !venue.is_empty())
        .map(|venue| venue.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if venues.is_empty() {
        return Err("--realtime-venues requires at least one venue".into());
    }
    for venue in &venues {
        validate_venue(venue, "--realtime-venues")?;
    }
    Ok(venues)
}

fn validate_venue(value: &str, name: &str) -> Result<(), Box<dyn Error>> {
    if value != "binance" && value != "upbit" {
        return Err(format!("{name} must contain only binance or upbit").into());
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
