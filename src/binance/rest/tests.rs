use super::url::market_data_url;

#[test]
fn market_data_url_preserves_existing_base_path_prefix() {
    let url = market_data_url("https://proxy.example/binance/", "/fapi/v1/openInterest").unwrap();

    assert_eq!(
        url.as_str(),
        "https://proxy.example/binance/fapi/v1/openInterest"
    );
}

#[test]
fn market_data_url_rejects_non_https_base_url() {
    let error = market_data_url("http://fapi.binance.com", "/fapi/v1/openInterest")
        .unwrap_err()
        .to_string();

    assert!(error.contains("https"));
}

#[test]
fn market_data_url_rejects_query_or_fragment_in_base_url() {
    for base_url in [
        "https://fapi.binance.com?existing=query",
        "https://fapi.binance.com#fragment",
    ] {
        let error = market_data_url(base_url, "/fapi/v1/openInterest")
            .unwrap_err()
            .to_string();

        assert!(error.contains("query or fragment"));
    }
}

#[test]
fn market_data_url_rejects_credentials_in_base_url() {
    let error = market_data_url(
        "https://user:secret@fapi.binance.com",
        "/fapi/v1/openInterest",
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("credentials"));
}

#[test]
fn market_data_url_rejects_query_or_fragment_in_endpoint() {
    for endpoint in [
        "/fapi/v1/openInterest?symbol=BTCUSDT",
        "/fapi/v1/openInterest#x",
    ] {
        let error = market_data_url("https://fapi.binance.com", endpoint)
            .unwrap_err()
            .to_string();

        assert!(error.contains("endpoint path"));
    }
}
