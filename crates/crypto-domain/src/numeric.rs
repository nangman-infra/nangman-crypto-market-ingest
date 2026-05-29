mod bps;
mod fixed_decimal;

pub use bps::{Bps, MicroBps};
pub use fixed_decimal::FixedDecimal;

pub type Price = FixedDecimal;
pub type Quantity = FixedDecimal;
pub type Notional = FixedDecimal;
pub type Ratio = FixedDecimal;
