use super::types::{AggTrade, BINANCE_PAGE_LIMIT};
use super::url::spot_market_data_url;
use crate::backfill::BackfillError;

const AGG_TRADES_ENDPOINT: &str = "/api/v3/aggTrades";

pub(super) async fn fetch_agg_trades_by_time(
    client: &reqwest::Client,
    rest_base_url: &str,
    symbol: &str,
    input_start_ms: i64,
    input_end_ms: i64,
) -> Result<Vec<AggTrade>, BackfillError> {
    client
        .get(spot_market_data_url(rest_base_url, AGG_TRADES_ENDPOINT)?)
        .query(&[
            ("symbol", symbol.to_owned()),
            ("startTime", input_start_ms.to_string()),
            ("endTime", input_end_ms.to_string()),
            ("limit", BINANCE_PAGE_LIMIT.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<AggTrade>>()
        .await
        .map_err(BackfillError::from)
}

pub(super) async fn fetch_agg_trades_from_id(
    client: &reqwest::Client,
    rest_base_url: &str,
    symbol: &str,
    from_id: i64,
) -> Result<Vec<AggTrade>, BackfillError> {
    client
        .get(spot_market_data_url(rest_base_url, AGG_TRADES_ENDPOINT)?)
        .query(&[
            ("symbol", symbol.to_owned()),
            ("fromId", from_id.to_string()),
            ("limit", BINANCE_PAGE_LIMIT.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<AggTrade>>()
        .await
        .map_err(BackfillError::from)
}
