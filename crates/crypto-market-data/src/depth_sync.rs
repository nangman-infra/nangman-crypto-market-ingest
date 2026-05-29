mod book;
mod sync;
mod types;

#[cfg(test)]
pub(crate) use self::book::sync_depth_book_from_snapshot;
pub(crate) use self::sync::handle_diff_depth_event;
pub(crate) use self::types::BinanceLocalOrderBook;
pub use self::types::{BinanceDepthSyncSettings, BinanceGapAlert};
