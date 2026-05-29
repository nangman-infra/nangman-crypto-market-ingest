use super::super::super::derivatives;
use super::BinanceMarket;
use crate::clock;
use crate::log_stream;
use crate::storage::L0StorageSink;

pub(super) async fn append_and_flush_derivative_snapshots(
    futures_rest_base_url: &str,
    markets: &[BinanceMarket],
    sink: &mut L0StorageSink,
) {
    match derivatives::append_derivative_snapshots(
        futures_rest_base_url,
        markets,
        clock::now_ms(),
        sink,
    )
    .await
    {
        Ok(report) => match sink.flush_all().await {
            Ok(()) => {
                let _ = log_stream::info(
                    "market_ingest_binance_derivative_snapshot_published",
                    serde_json::json!({
                        "funding_rate_snapshot_records": report.funding_rate_snapshot_records,
                        "open_interest_snapshot_records": report.open_interest_snapshot_records,
                        "derivative_snapshot_records": report.total_records(),
                        "unsupported_futures_symbol_count": report.unsupported_futures_symbol_count,
                        "failure_count": report.failure_count,
                    }),
                );
            }
            Err(error) => {
                let _ = log_stream::warn(
                    "market_ingest_binance_derivative_snapshot_flush_failed",
                    serde_json::json!({
                        "error": error.to_string()
                    }),
                );
            }
        },
        Err(error) => {
            let _ = log_stream::warn(
                "market_ingest_binance_derivative_snapshot_publish_failed",
                serde_json::json!({
                    "error": error.to_string()
                }),
            );
        }
    }
}
