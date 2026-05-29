use crate::error::MarketDataError;
use crate::stream_config::BinanceStreamKind;

pub(super) fn ensure_partial_depth_kind(
    kind: BinanceStreamKind,
    error_message: &'static str,
) -> Result<(), MarketDataError> {
    if matches!(
        kind,
        BinanceStreamKind::PartialDepth5
            | BinanceStreamKind::PartialDepth10
            | BinanceStreamKind::PartialDepth20
    ) {
        Ok(())
    } else {
        Err(MarketDataError::InvalidMessage(error_message.to_owned()))
    }
}
