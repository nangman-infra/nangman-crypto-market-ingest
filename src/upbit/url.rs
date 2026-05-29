use super::UpbitIngestError;

pub(super) fn upbit_market_data_url(
    base_url: &str,
    endpoint_path: &str,
) -> Result<reqwest::Url, UpbitIngestError> {
    validate_endpoint_path(endpoint_path)?;
    let base = reqwest::Url::parse(base_url.trim()).map_err(|error| {
        UpbitIngestError::InvalidConfig(format!("invalid Upbit REST base URL: {error}"))
    })?;
    validate_base_url(&base)?;
    reqwest::Url::parse(&format!(
        "{}{}",
        base.as_str().trim_end_matches('/'),
        endpoint_path
    ))
    .map_err(|error| {
        UpbitIngestError::InvalidConfig(format!("invalid Upbit REST endpoint URL: {error}"))
    })
}

fn validate_base_url(base: &reqwest::Url) -> Result<(), UpbitIngestError> {
    if base.scheme() != "https" {
        return Err(UpbitIngestError::InvalidConfig(
            "Upbit REST base URL must use https".to_owned(),
        ));
    }
    if base.host_str().is_none() {
        return Err(UpbitIngestError::InvalidConfig(
            "Upbit REST base URL must include a host".to_owned(),
        ));
    }
    if !base.username().is_empty() || base.password().is_some() {
        return Err(UpbitIngestError::InvalidConfig(
            "Upbit REST base URL must not include credentials".to_owned(),
        ));
    }
    if base.query().is_some() || base.fragment().is_some() {
        return Err(UpbitIngestError::InvalidConfig(
            "Upbit REST base URL must not include query or fragment components".to_owned(),
        ));
    }
    Ok(())
}

fn validate_endpoint_path(endpoint_path: &str) -> Result<(), UpbitIngestError> {
    if !endpoint_path.starts_with('/') {
        return Err(UpbitIngestError::InvalidConfig(
            "Upbit REST endpoint path must start with /".to_owned(),
        ));
    }
    if endpoint_path.contains('?') || endpoint_path.contains('#') {
        return Err(UpbitIngestError::InvalidConfig(
            "Upbit REST endpoint path must not contain query or fragment components".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::upbit_market_data_url;

    #[test]
    fn upbit_market_data_url_preserves_base_path_prefix() {
        let url = upbit_market_data_url("https://proxy.example/upbit/", "/v1/market/all").unwrap();

        assert_eq!(url.as_str(), "https://proxy.example/upbit/v1/market/all");
    }

    #[test]
    fn upbit_market_data_url_rejects_non_https_base_url() {
        let error = upbit_market_data_url("http://api.upbit.com", "/v1/market/all")
            .unwrap_err()
            .to_string();

        assert!(error.contains("https"));
    }

    #[test]
    fn upbit_market_data_url_rejects_query_or_fragment_in_base_url() {
        for base_url in [
            "https://api.upbit.com?existing=query",
            "https://api.upbit.com#fragment",
        ] {
            let error = upbit_market_data_url(base_url, "/v1/market/all")
                .unwrap_err()
                .to_string();

            assert!(error.contains("query or fragment"));
        }
    }

    #[test]
    fn upbit_market_data_url_rejects_credentials_in_base_url() {
        let error = upbit_market_data_url("https://user:secret@api.upbit.com", "/v1/market/all")
            .unwrap_err()
            .to_string();

        assert!(error.contains("credentials"));
    }

    #[test]
    fn upbit_market_data_url_rejects_query_or_fragment_in_endpoint() {
        for endpoint in ["/v1/market/all?is_details=true", "/v1/market/all#fragment"] {
            let error = upbit_market_data_url("https://api.upbit.com", endpoint)
                .unwrap_err()
                .to_string();

            assert!(error.contains("endpoint path"));
        }
    }
}
