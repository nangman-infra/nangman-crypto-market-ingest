use super::super::book::sync_depth_book_from_snapshot;
use super::super::types::{BinanceDepthSyncSettings, BinanceLocalOrderBook};
use super::alerts::{record_delta_parse_gap, record_snapshot_fetch_failure};
use crate::error::MarketDataError;
use crate::messages::BinanceOrderBookSnapshot;
use crate::stats::BinanceIngestWatchStats;
use crypto_domain::TimestampMs;
use std::collections::{BTreeMap, HashSet};

pub(super) fn should_fetch_snapshot(
    books: &BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    raw_symbol: &str,
) -> bool {
    books.get(raw_symbol).is_some_and(|book| !book.is_synced())
        && snapshot_attempted.insert(raw_symbol.to_owned())
}

pub(super) async fn fetch_and_sync_snapshot(
    depth_sync: &BinanceDepthSyncSettings,
    http_client: &reqwest::Client,
    books: &mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
) -> Result<(), MarketDataError> {
    stats.depth_snapshot_requests += 1;
    match fetch_binance_depth_snapshot(http_client, depth_sync, raw_symbol).await {
        Ok(snapshot) => sync_fetched_snapshot(
            books,
            snapshot_attempted,
            stats,
            raw_symbol,
            received_time_ms,
            snapshot,
        ),
        Err(error) => record_snapshot_fetch_failure(stats, raw_symbol, received_time_ms, error),
    }
    Ok(())
}

fn sync_fetched_snapshot(
    books: &mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    snapshot: BinanceOrderBookSnapshot,
) {
    stats.depth_snapshot_successes += 1;
    let Some(book) = books.get_mut(raw_symbol) else {
        return;
    };
    if let Err(alert) = sync_depth_book_from_snapshot(book, snapshot) {
        record_delta_parse_gap(stats, raw_symbol, received_time_ms, *alert);
        snapshot_attempted.remove(raw_symbol);
    }
}

async fn fetch_binance_depth_snapshot(
    client: &reqwest::Client,
    depth_sync: &BinanceDepthSyncSettings,
    raw_symbol: &str,
) -> Result<BinanceOrderBookSnapshot, MarketDataError> {
    let url = binance_depth_snapshot_url(depth_sync, raw_symbol)?;
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<BinanceOrderBookSnapshot>()
        .await?)
}

fn binance_depth_snapshot_url(
    depth_sync: &BinanceDepthSyncSettings,
    raw_symbol: &str,
) -> Result<reqwest::Url, MarketDataError> {
    let base = reqwest::Url::parse(depth_sync.rest_base_url.trim()).map_err(|error| {
        MarketDataError::InvalidMessage(format!("invalid Binance depth REST base URL: {error}"))
    })?;
    validate_binance_depth_rest_base_url(&base)?;

    let mut url = reqwest::Url::parse(&format!(
        "{}/api/v3/depth",
        base.as_str().trim_end_matches('/')
    ))
    .map_err(|error| {
        MarketDataError::InvalidMessage(format!("invalid Binance depth REST URL: {error}"))
    })?;
    url.query_pairs_mut()
        .append_pair("symbol", raw_symbol)
        .append_pair("limit", &depth_sync.snapshot_limit.to_string());
    Ok(url)
}

fn validate_binance_depth_rest_base_url(base: &reqwest::Url) -> Result<(), MarketDataError> {
    if base.scheme() != "https" {
        return Err(MarketDataError::InvalidMessage(
            "Binance depth REST base URL must use https".to_owned(),
        ));
    }
    if base.host_str().is_none() {
        return Err(MarketDataError::InvalidMessage(
            "Binance depth REST base URL must include a host".to_owned(),
        ));
    }
    if !base.username().is_empty() || base.password().is_some() {
        return Err(MarketDataError::InvalidMessage(
            "Binance depth REST base URL must not include credentials".to_owned(),
        ));
    }
    if base.query().is_some() || base.fragment().is_some() {
        return Err(MarketDataError::InvalidMessage(
            "Binance depth REST base URL must not include query or fragment components".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BinanceDepthSyncSettings, binance_depth_snapshot_url};

    fn settings(rest_base_url: &str) -> BinanceDepthSyncSettings {
        BinanceDepthSyncSettings {
            rest_base_url: rest_base_url.to_owned(),
            snapshot_limit: 1_000,
        }
    }

    #[test]
    fn binance_depth_snapshot_url_preserves_base_path_prefix() {
        let url =
            binance_depth_snapshot_url(&settings("https://proxy.example/binance/"), "BTCUSDT")
                .unwrap();

        assert_eq!(
            url.as_str(),
            "https://proxy.example/binance/api/v3/depth?symbol=BTCUSDT&limit=1000"
        );
    }

    #[test]
    fn binance_depth_snapshot_url_encodes_symbol_query_value() {
        let url =
            binance_depth_snapshot_url(&settings("https://api.binance.com"), "BTCUSDT&limit=1")
                .unwrap();

        assert_eq!(
            url.as_str(),
            "https://api.binance.com/api/v3/depth?symbol=BTCUSDT%26limit%3D1&limit=1000"
        );
    }

    #[test]
    fn binance_depth_snapshot_url_rejects_non_https_base_url() {
        let error = binance_depth_snapshot_url(&settings("http://api.binance.com"), "BTCUSDT")
            .unwrap_err()
            .to_string();

        assert!(error.contains("https"));
    }

    #[test]
    fn binance_depth_snapshot_url_rejects_credentials_in_base_url() {
        let error =
            binance_depth_snapshot_url(&settings("https://user:secret@api.binance.com"), "BTCUSDT")
                .unwrap_err()
                .to_string();

        assert!(error.contains("credentials"));
    }

    #[test]
    fn binance_depth_snapshot_url_rejects_query_or_fragment_in_base_url() {
        for base_url in [
            "https://api.binance.com?existing=query",
            "https://api.binance.com#fragment",
        ] {
            let error = binance_depth_snapshot_url(&settings(base_url), "BTCUSDT")
                .unwrap_err()
                .to_string();

            assert!(error.contains("query or fragment"));
        }
    }
}
