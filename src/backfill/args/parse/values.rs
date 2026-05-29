use super::super::types::Venue;
use crate::backfill::BackfillError;
use std::path::PathBuf;

pub(super) fn next_required_arg(
    args: &mut impl Iterator<Item = String>,
    message: &'static str,
) -> Result<String, BackfillError> {
    args.next()
        .ok_or_else(|| BackfillError::InvalidArgs(message.to_owned()))
}

pub(super) fn parse_absolute_path(
    value: String,
    message: &'static str,
) -> Result<PathBuf, BackfillError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(BackfillError::InvalidArgs(message.to_owned()));
    }
    Ok(path)
}

pub(super) fn parse_venue(value: String) -> Result<Venue, BackfillError> {
    match value.as_str() {
        "binance" => Ok(Venue::Binance),
        "upbit" => Ok(Venue::Upbit),
        _ => Err(BackfillError::InvalidArgs(
            "--venue must be binance or upbit".to_owned(),
        )),
    }
}

pub(super) fn parse_symbols(raw: &str) -> Result<Vec<String>, BackfillError> {
    let parsed = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if parsed.is_empty() {
        return Err(BackfillError::InvalidArgs(
            "--symbols requires at least one symbol".to_owned(),
        ));
    }
    Ok(parsed)
}
