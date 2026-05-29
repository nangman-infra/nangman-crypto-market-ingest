use super::types::UpbitBackfillMarket;
use crate::backfill::{BackfillArgs, BackfillError};
use crate::upbit::fetch_top_krw_markets;
use std::time::Duration;

pub(super) async fn resolve_rest_base_url(args: &BackfillArgs) -> Result<String, BackfillError> {
    if let Some(url) = &args.rest_base_url {
        return Ok(url.clone());
    }
    let config = crate::config::load_market_ingest_config(&args.config_dir)
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    let exchange = config
        .enabled_exchange("upbit")
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    Ok(exchange.rest_base_url.clone())
}

pub(super) async fn resolve_markets(
    args: &BackfillArgs,
) -> Result<Vec<UpbitBackfillMarket>, BackfillError> {
    if args.upbit_quote_currency != "KRW" {
        return Err(BackfillError::InvalidArgs(
            "Upbit historical backfill only supports KRW quote markets".to_owned(),
        ));
    }
    if let Some(symbols) = &args.symbols {
        return symbols
            .iter()
            .map(|symbol| parse_explicit_market(symbol, &args.upbit_quote_currency))
            .collect();
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let rest_base_url = resolve_rest_base_url(args).await?;
    let markets = fetch_top_krw_markets(
        &client,
        &rest_base_url,
        &args.upbit_quote_currency,
        args.expect_symbol_count,
    )
    .await
    .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    Ok(markets
        .into_iter()
        .map(|market| UpbitBackfillMarket {
            market: market.market,
            base_asset: market.base_asset,
            quote_asset: market.quote_asset,
        })
        .collect())
}

fn parse_explicit_market(
    value: &str,
    expected_quote: &str,
) -> Result<UpbitBackfillMarket, BackfillError> {
    let Some((quote, base)) = value.split_once('-') else {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit symbol must look like KRW-BTC, got {value}"
        )));
    };
    if quote != expected_quote {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit symbol {value} must use {expected_quote} quote"
        )));
    }
    Ok(UpbitBackfillMarket {
        market: value.to_owned(),
        base_asset: base.to_owned(),
        quote_asset: quote.to_owned(),
    })
}
