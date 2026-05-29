mod event;
mod health;
mod model;
#[cfg(test)]
mod tests;

pub use model::{BinanceL0GapAlert, BinanceL0WatchStats};

const MAX_STORED_GAP_ALERTS: usize = 1_000;
const MAX_RECENT_GAP_ALERTS: usize = 20;
