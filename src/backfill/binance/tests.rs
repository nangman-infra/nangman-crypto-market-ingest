use super::trade::raw_trade_draft;
use super::types::AggTrade;
use super::url::spot_market_data_url;
use crate::binance::BinanceMarket;

fn sample_market() -> BinanceMarket {
    BinanceMarket {
        raw_symbol: "BTCUSDT".to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "USDT".to_owned(),
    }
}

#[test]
fn raw_trade_payload_matches_normalizer_shape() {
    let trade = AggTrade {
        aggregate_trade_id: 42,
        price: "100.25".to_owned(),
        quantity: "0.50".to_owned(),
        first_trade_id: 7,
        last_trade_id: 8,
        trade_timestamp_ms: 1234,
        is_buyer_maker: true,
        is_best_match: true,
    };
    let draft = raw_trade_draft(&sample_market(), &trade).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&draft.payload_json).unwrap();
    assert_eq!(payload["data"]["p"], "100.25");
    assert_eq!(payload["data"]["q"], "0.50");
    assert_eq!(payload["data"]["m"], true);
    assert_eq!(draft.stream_type, "HISTORICAL_REST");
}

#[test]
fn spot_market_data_url_preserves_base_path_prefix() {
    let url = spot_market_data_url("https://proxy.example/binance/", "/api/v3/aggTrades").unwrap();

    assert_eq!(
        url.as_str(),
        "https://proxy.example/binance/api/v3/aggTrades"
    );
}

#[test]
fn spot_market_data_url_rejects_non_https_base_url() {
    let error = spot_market_data_url("http://api.binance.com", "/api/v3/aggTrades")
        .unwrap_err()
        .to_string();

    assert!(error.contains("https"));
}

#[test]
fn spot_market_data_url_rejects_query_or_fragment_in_base_url() {
    for base_url in [
        "https://api.binance.com?existing=query",
        "https://api.binance.com#fragment",
    ] {
        let error = spot_market_data_url(base_url, "/api/v3/aggTrades")
            .unwrap_err()
            .to_string();

        assert!(error.contains("query or fragment"));
    }
}

#[test]
fn spot_market_data_url_rejects_credentials_in_base_url() {
    let error = spot_market_data_url("https://user:secret@api.binance.com", "/api/v3/aggTrades")
        .unwrap_err()
        .to_string();

    assert!(error.contains("credentials"));
}
