use std::error::Error;

pub(super) fn parse_i64_arg(value: Option<String>, name: &str) -> Result<i64, Box<dyn Error>> {
    value
        .ok_or_else(|| format!("{name} requires an integer"))?
        .parse::<i64>()
        .map_err(|_| format!("{name} must be an integer").into())
}

pub(super) fn parse_positive_i64(value: Option<String>, name: &str) -> Result<i64, Box<dyn Error>> {
    let parsed = parse_i64_arg(value, name)?;
    if parsed <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_u64(value: Option<String>, name: &str) -> Result<u64, Box<dyn Error>> {
    let parsed = value
        .ok_or_else(|| format!("{name} requires a positive integer"))?
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_usize(
    value: Option<String>,
    name: &str,
) -> Result<usize, Box<dyn Error>> {
    let parsed = value
        .ok_or_else(|| format!("{name} requires a positive integer"))?
        .parse::<usize>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}
