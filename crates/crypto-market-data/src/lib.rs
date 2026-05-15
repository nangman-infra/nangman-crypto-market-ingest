mod clock;
mod depth_sync;
mod error;
mod ingest_watch;
mod messages;
mod normalize;
mod reader;
mod stats;
mod stream_config;
mod tls;

pub use depth_sync::{BinanceDepthSyncSettings, BinanceGapAlert};
pub use error::MarketDataError;
pub use ingest_watch::{
    watch_binance_ingest_streams, watch_binance_ingest_streams_with_depth_sync,
};
pub use normalize::{
    normalize_binance_partial_depth_message, normalize_binance_stream_message,
    normalize_binance_ticker_message,
};
pub use reader::{
    read_one_binance_partial_depth_snapshot, read_one_binance_ticker_snapshot,
    stream_binance_ticker_and_partial_depth_snapshots, stream_binance_ticker_snapshots,
};
pub use stats::{BinanceIngestWatchStats, MarketStreamStats};
pub use stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig, BinanceStreamKind};

const MAX_CLOCK_SKEW_MS: i64 = 5_000;

#[cfg(test)]
pub(crate) use crypto_domain::{Bps, EventQuality, FixedDecimal, Symbol};
#[cfg(test)]
pub(crate) use depth_sync::{BinanceLocalOrderBook, sync_depth_book_from_snapshot};
#[cfg(test)]
pub(crate) use ingest_watch::observe_binance_ingest_payload;
#[cfg(test)]
pub(crate) use messages::{BinanceDiffDepthMessage, BinanceOrderBookSnapshot};

#[cfg(test)]
mod tests;
