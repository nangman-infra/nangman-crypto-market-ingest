use super::super::args::InputRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Live,
    CatchUp,
    Backfill,
}

impl RunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RunMode::Live => "LIVE",
            RunMode::CatchUp => "CATCH-UP",
            RunMode::Backfill => "BACKFILL",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RunDecision {
    pub run_mode: RunMode,
    pub input_range: InputRange,
}
