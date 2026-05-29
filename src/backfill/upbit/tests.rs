use super::cursor::{initial_cursor, validate_recent_window};
use super::trade::raw_trade_draft;
use super::types::{UpbitBackfillMarket, UpbitTrade};
use super::url::upbit_market_data_url;
use chrono::{Duration as ChronoDuration, Timelike, Utc};

#[test]
fn initial_cursor_uses_days_ago_for_prior_day() {
    let now = Utc::now();
    let end = (now - ChronoDuration::days(1))
        .with_hour(8)
        .unwrap()
        .with_minute(10)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let cursor = initial_cursor(end.timestamp_millis()).unwrap();
    assert_eq!(cursor.to, "08:10:00");
    assert_eq!(cursor.days_ago, Some(1));
}

#[test]
fn rejects_range_older_than_recent_window() {
    let now = Utc::now();
    let start = (now - ChronoDuration::days(8)).timestamp_millis();
    let end = (now - ChronoDuration::days(7)).timestamp_millis();
    let err = validate_recent_window(start, end).err().unwrap();
    assert!(err.to_string().contains("most recent 7 days"));
}

#[test]
fn raw_trade_payload_matches_normalizer_shape() {
    let market = UpbitBackfillMarket {
        market: "KRW-BTC".to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "KRW".to_owned(),
    };
    let trade = UpbitTrade {
        market: "KRW-BTC".to_owned(),
        timestamp: 1234,
        trade_price: 100.0,
        trade_volume: 0.25,
        ask_bid: "BID".to_owned(),
        sequential_id: 99,
    };
    let draft = raw_trade_draft(&market, &trade).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&draft.payload_json).unwrap();
    assert_eq!(payload["trade_price"], 100.0);
    assert_eq!(payload["trade_volume"], 0.25);
    assert_eq!(payload["ask_bid"], "BID");
}

#[test]
fn upbit_market_data_url_preserves_base_path_prefix() {
    let url = upbit_market_data_url("https://proxy.example/upbit/", "/v1/trades/ticks").unwrap();

    assert_eq!(url.as_str(), "https://proxy.example/upbit/v1/trades/ticks");
}

#[test]
fn upbit_market_data_url_rejects_non_https_base_url() {
    let error = upbit_market_data_url("http://api.upbit.com", "/v1/trades/ticks")
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
        let error = upbit_market_data_url(base_url, "/v1/trades/ticks")
            .unwrap_err()
            .to_string();

        assert!(error.contains("query or fragment"));
    }
}

#[test]
fn upbit_market_data_url_rejects_credentials_in_base_url() {
    let error = upbit_market_data_url("https://user:secret@api.upbit.com", "/v1/trades/ticks")
        .unwrap_err()
        .to_string();

    assert!(error.contains("credentials"));
}
