use super::types::{UpbitInitialCursor, UpbitTrade};
use super::url::upbit_market_data_url;
use crate::backfill::BackfillError;

pub(super) const UPBIT_PAGE_LIMIT: usize = 200;
const TRADES_TICKS_ENDPOINT: &str = "/v1/trades/ticks";

pub(super) async fn fetch_trade_page(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &str,
    initial_cursor: &UpbitInitialCursor,
    cursor: Option<i64>,
    first_page: bool,
) -> Result<Vec<UpbitTrade>, BackfillError> {
    let mut request = client
        .get(upbit_market_data_url(rest_base_url, TRADES_TICKS_ENDPOINT)?)
        .query(&[
            ("market", market.to_owned()),
            ("count", UPBIT_PAGE_LIMIT.to_string()),
        ]);
    if first_page {
        request = request.query(&[("to", initial_cursor.to.clone())]);
        if let Some(days_ago) = initial_cursor.days_ago {
            request = request.query(&[("days_ago", days_ago.to_string())]);
        }
    }
    if let Some(cursor) = cursor {
        request = request.query(&[("cursor", cursor.to_string())]);
    }
    request
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<UpbitTrade>>()
        .await
        .map_err(BackfillError::from)
}
