#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArchiveKind {
    Events,
    Candles,
}

impl ArchiveKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Candles => "candles",
        }
    }

    pub fn from_segment(segment: &str) -> Option<Self> {
        match segment.strip_prefix("kind=").unwrap_or(segment) {
            "events" => Some(Self::Events),
            "candles" => Some(Self::Candles),
            _ => None,
        }
    }
}
