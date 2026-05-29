mod basic;
mod depth;

#[allow(deprecated)]
pub use basic::watch_binance_ingest_streams;
#[allow(deprecated)]
pub use depth::watch_binance_ingest_streams_with_depth_sync;
