use super::UpbitIngestError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize)]
pub struct UpbitMarket {
    pub market: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub korean_name: String,
    pub english_name: String,
    pub acc_trade_price_24h: f64,
}

#[derive(Debug, Deserialize)]
struct MarketAllEntry {
    market: String,
    korean_name: String,
    english_name: String,
    market_event: Option<MarketEvent>,
}

#[derive(Debug, Deserialize)]
struct MarketEvent {
    warning: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct TickerAllEntry {
    market: String,
    acc_trade_price_24h: f64,
}

pub async fn fetch_top_krw_markets(
    http: &reqwest::Client,
    rest_base_url: &str,
    quote_currency: &str,
    limit: usize,
) -> Result<Vec<UpbitMarket>, UpbitIngestError> {
    let rest_base_url = rest_base_url.trim_end_matches('/');
    let market_url = format!("{rest_base_url}/v1/market/all?is_details=true");
    let ticker_url = format!("{rest_base_url}/v1/ticker/all?quote_currencies={quote_currency}");

    let markets = http
        .get(market_url)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<MarketAllEntry>>()
        .await?;
    let tickers = http
        .get(ticker_url)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<TickerAllEntry>>()
        .await?;

    let active_markets = active_quote_markets(markets, quote_currency);
    let ticker_by_market = tickers
        .into_iter()
        .map(|ticker| (ticker.market.clone(), ticker.acc_trade_price_24h))
        .collect::<HashMap<_, _>>();

    let mut ranked = active_markets
        .into_iter()
        .filter_map(|market| {
            let acc_trade_price_24h = *ticker_by_market.get(&market.market)?;
            Some(UpbitMarket {
                base_asset: base_asset(&market.market, quote_currency),
                quote_asset: quote_currency.to_owned(),
                market: market.market,
                korean_name: market.korean_name,
                english_name: market.english_name,
                acc_trade_price_24h,
            })
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .acc_trade_price_24h
            .partial_cmp(&left.acc_trade_price_24h)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if ranked.len() < limit {
        return Err(UpbitIngestError::InvalidMessage(format!(
            "Upbit returned only {} active {quote_currency} markets, expected at least {limit}",
            ranked.len()
        )));
    }
    ranked.truncate(limit);
    Ok(ranked)
}

fn active_quote_markets(markets: Vec<MarketAllEntry>, quote_currency: &str) -> Vec<MarketAllEntry> {
    let prefix = format!("{quote_currency}-");
    let mut seen = HashSet::new();
    markets
        .into_iter()
        .filter(|market| market.market.starts_with(&prefix))
        .filter(|market| {
            !market
                .market_event
                .as_ref()
                .and_then(|event| event.warning)
                .unwrap_or(false)
        })
        .filter(|market| seen.insert(market.market.clone()))
        .collect()
}

fn base_asset(market: &str, quote_currency: &str) -> String {
    market
        .strip_prefix(&format!("{quote_currency}-"))
        .unwrap_or(market)
        .to_owned()
}
