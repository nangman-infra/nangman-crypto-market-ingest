#[derive(Debug, Clone, Copy)]
pub(super) enum InputObjectFamily {
    RawMarketEvent,
    SymbolHealth,
    SourceHealth,
    GapAlert,
}

impl InputObjectFamily {
    pub(super) fn from_key(key: &str) -> Option<Self> {
        if key.starts_with("raw_market_event/") {
            Some(Self::RawMarketEvent)
        } else if key.starts_with("symbol_health/") {
            Some(Self::SymbolHealth)
        } else if key.starts_with("source_health/") {
            Some(Self::SourceHealth)
        } else if key.starts_with("gap_alert/") {
            Some(Self::GapAlert)
        } else {
            None
        }
    }
}
