use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bps {
    pub value: i64,
}

impl Bps {
    pub const fn new(value: i64) -> Self {
        Self { value }
    }
}

impl Default for Bps {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MicroBps {
    pub value: i64,
}

impl MicroBps {
    pub const fn new(value: i64) -> Self {
        Self { value }
    }
}

impl Default for MicroBps {
    fn default() -> Self {
        Self::new(0)
    }
}
