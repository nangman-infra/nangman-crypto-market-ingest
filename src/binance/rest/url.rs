use super::super::BinanceIngestError;

pub(super) fn market_data_url(
    base_url: &str,
    endpoint_path: &str,
) -> Result<reqwest::Url, BinanceIngestError> {
    validate_endpoint_path(endpoint_path)?;
    let base = reqwest::Url::parse(base_url.trim()).map_err(|error| {
        BinanceIngestError::InvalidMessage(format!("invalid Binance REST base URL: {error}"))
    })?;
    validate_base_url(&base)?;
    reqwest::Url::parse(&format!(
        "{}{}",
        base.as_str().trim_end_matches('/'),
        endpoint_path
    ))
    .map_err(|error| {
        BinanceIngestError::InvalidMessage(format!("invalid Binance REST endpoint URL: {error}"))
    })
}

fn validate_base_url(base: &reqwest::Url) -> Result<(), BinanceIngestError> {
    if base.scheme() != "https" {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance REST base URL must use https".to_owned(),
        ));
    }
    if base.host_str().is_none() {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance REST base URL must include a host".to_owned(),
        ));
    }
    if !base.username().is_empty() || base.password().is_some() {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance REST base URL must not include credentials".to_owned(),
        ));
    }
    if base.query().is_some() || base.fragment().is_some() {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance REST base URL must not include query or fragment components".to_owned(),
        ));
    }
    Ok(())
}

fn validate_endpoint_path(endpoint_path: &str) -> Result<(), BinanceIngestError> {
    if !endpoint_path.starts_with('/') {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance REST endpoint path must start with /".to_owned(),
        ));
    }
    if endpoint_path.contains('?') || endpoint_path.contains('#') {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance REST endpoint path must not contain query or fragment components".to_owned(),
        ));
    }
    Ok(())
}
