use super::super::universe::UpbitMarket;
use crate::clock;
use serde_json::json;

pub(super) fn subscription_message(markets: &[UpbitMarket], orderbook_unit: u8) -> String {
    let codes = markets
        .iter()
        .map(|market| market.market.clone())
        .collect::<Vec<_>>();
    let orderbook_codes = markets
        .iter()
        .map(|market| format!("{}.{}", market.market, orderbook_unit))
        .collect::<Vec<_>>();
    json!([
        {"ticket": format!("nangman-market-ingest-upbit-{}", clock::now_ms())},
        {"type": "ticker", "codes": codes},
        {"type": "trade", "codes": codes},
        {"type": "orderbook", "codes": orderbook_codes},
        {"format": "DEFAULT"}
    ])
    .to_string()
}
