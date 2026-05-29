use super::super::BinanceIngestError;
use super::super::stats::BinanceL0WatchStats;
use super::gaps::gap_alert_draft;
use super::health::{source_health_draft, symbol_health_drafts};
use crate::storage::L0StorageSink;

pub(super) async fn finalize_storage(
    sink: &mut L0StorageSink,
    stats: &BinanceL0WatchStats,
) -> Result<(), BinanceIngestError> {
    sink.append_source_health(source_health_draft(stats))
        .await
        .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    for draft in symbol_health_drafts(stats) {
        sink.append_symbol_health(draft)
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    }
    for alert in &stats.gap_alerts {
        sink.append_gap_alert(gap_alert_draft(alert))
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    }
    sink.flush_all()
        .await
        .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    sink.upload_manifest()
        .await
        .map_err(|error| BinanceIngestError::Storage(error.to_string()))
}
