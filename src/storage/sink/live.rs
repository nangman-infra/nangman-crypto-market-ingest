use super::config::live_nats_required;
use super::{L0StorageSink, StorageError};
use crate::live::MarketLiveTick;
use crate::log_stream;
use crate::storage::record::RawMarketEventRecord;
use serde_json::json;

impl L0StorageSink {
    pub(super) async fn publish_live_tick(
        &self,
        record: &RawMarketEventRecord,
    ) -> Result<(), StorageError> {
        let Some(publisher) = &self.live_publisher else {
            return Ok(());
        };
        let tick = MarketLiveTick::from_raw_market_event(record);
        if !tick.has_mark_price() {
            return Ok(());
        }
        match publisher.publish_tick(&tick).await {
            Ok(()) => Ok(()),
            Err(error) if live_nats_required(&self.config) => Err(StorageError::Nats(format!(
                "publish market live tick {}: {error}",
                tick.event_id
            ))),
            Err(error) => {
                let _ = log_stream::warn(
                    "market_live_tick_publish_failed",
                    json!({
                        "event_id": tick.event_id,
                        "venue": tick.venue,
                        "symbol": tick.symbol_canonical,
                        "error": error.to_string(),
                        "required": false
                    }),
                );
                Ok(())
            }
        }
    }
}
