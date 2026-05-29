use super::UpbitIngestError;
use crate::storage::L0StorageConfig;

#[derive(Debug, Clone)]
pub struct UpbitRunConfig {
    pub rest_base_url: String,
    pub websocket_url: String,
    pub quote_currency: String,
    pub duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub expect_symbol_count: usize,
    pub allow_partial_symbol_coverage: bool,
    pub orderbook_unit: u8,
    pub l0_storage: Option<L0StorageConfig>,
}

pub(super) fn validate_config(config: &UpbitRunConfig) -> Result<(), UpbitIngestError> {
    if config.quote_currency != "KRW" {
        return Err(UpbitIngestError::InvalidConfig(
            "initial Upbit L0 ingest only supports KRW quote markets".to_owned(),
        ));
    }
    if !matches!(config.orderbook_unit, 1 | 5 | 15 | 30) {
        return Err(UpbitIngestError::InvalidConfig(
            "upbit orderbook unit must be one of 1, 5, 15, or 30".to_owned(),
        ));
    }
    Ok(())
}
