use super::Venue;
use std::error::Error;

pub(super) fn parse_venue(value: String) -> Result<Venue, Box<dyn Error>> {
    match value.as_str() {
        "binance" => Ok(Venue::Binance),
        "upbit" => Ok(Venue::Upbit),
        _ => Err("--venue must be binance or upbit".into()),
    }
}

pub(super) fn parse_depth_snapshot_limit(value: Option<String>) -> Result<u16, Box<dyn Error>> {
    let parsed = value
        .ok_or("--depth-snapshot-limit requires 5, 10, 20, 50, 100, 500, 1000, or 5000")?
        .parse::<u16>()
        .map_err(|_| "--depth-snapshot-limit must be an integer")?;
    if !matches!(parsed, 5 | 10 | 20 | 50 | 100 | 500 | 1000 | 5000) {
        return Err("--depth-snapshot-limit must be 5, 10, 20, 50, 100, 500, 1000, or 5000".into());
    }
    Ok(parsed)
}

pub(super) fn parse_upbit_orderbook_unit(value: Option<String>) -> Result<u8, Box<dyn Error>> {
    let parsed = value
        .ok_or("--upbit-orderbook-unit requires 1, 5, 15, or 30")?
        .parse::<u8>()
        .map_err(|_| "--upbit-orderbook-unit must be an integer")?;
    if !matches!(parsed, 1 | 5 | 15 | 30) {
        return Err("--upbit-orderbook-unit must be 1, 5, 15, or 30".into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_u64(value: String, name: &str) -> Result<u64, Box<dyn Error>> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_usize(value: String, name: &str) -> Result<usize, Box<dyn Error>> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_u16(value: String, name: &str) -> Result<u16, Box<dyn Error>> {
    let parsed = value
        .parse::<u16>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_positive_i64(value: String, name: &str) -> Result<i64, Box<dyn Error>> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(parsed)
}

pub(super) fn parse_pct(value: String, name: &str) -> Result<u8, Box<dyn Error>> {
    let parsed = value
        .parse::<u8>()
        .map_err(|_| format!("{name} must be 1..100"))?;
    if parsed == 0 || parsed > 100 {
        return Err(format!("{name} must be 1..100").into());
    }
    Ok(parsed)
}
