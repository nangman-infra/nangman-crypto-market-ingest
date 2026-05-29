use super::funding::funding_snapshot_batch_from_value;
use crate::binance::BinanceMarket;
use serde_json::json;

fn market(symbol: &str) -> BinanceMarket {
    BinanceMarket {
        raw_symbol: symbol.to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "USDT".to_owned(),
    }
}

#[test]
fn funding_batch_accepts_array_response_and_tracks_supported_symbols() {
    let batch = funding_snapshot_batch_from_value(
        json!([
            {"symbol": "BTCUSDT", "lastFundingRate": "0.0001", "time": 7200000},
            {"symbol": "ETHUSDT", "lastFundingRate": "0.0002", "time": 7200001}
        ]),
        &[market("BTCUSDT")],
        7_200_005,
    )
    .unwrap();

    assert_eq!(batch.drafts.len(), 1);
    assert!(batch.supported_symbols.contains("BTCUSDT"));
    assert!(batch.supported_symbols.contains("ETHUSDT"));
    assert_eq!(
        batch.drafts[0].sequence_tag,
        "binance:funding_rate:BTCUSDT:7200000"
    );
}

#[test]
fn funding_batch_accepts_single_object_response() {
    let batch = funding_snapshot_batch_from_value(
        json!({"symbol": "BTCUSDT", "lastFundingRate": 0.0001, "time": 7200000}),
        &[market("BTCUSDT")],
        7_200_005,
    )
    .unwrap();

    assert_eq!(batch.drafts.len(), 1);
    assert_eq!(batch.drafts[0].event_type, "funding_rate_snapshot");
}

#[test]
fn funding_batch_rejects_non_record_response() {
    let error = funding_snapshot_batch_from_value(json!("bad"), &[market("BTCUSDT")], 7_200_005)
        .unwrap_err()
        .to_string();

    assert!(error.contains("premiumIndex"));
}
