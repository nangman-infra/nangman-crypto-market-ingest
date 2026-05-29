mod entry;
mod local;
mod s3;
#[cfg(test)]
mod tests;
mod time;

pub(crate) use entry::{InputEntry, InputEntrySource, collect_input_entries};

pub(crate) const VENUES: &[&str] = &["upbit", "binance"];
pub(crate) const RAW_EVENT_TYPES: &[&str] = &[
    "trade",
    "book_ticker",
    "depth_delta",
    "depth_snapshot",
    "ticker",
    "funding_rate_snapshot",
    "open_interest_snapshot",
];
