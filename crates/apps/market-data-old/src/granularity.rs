use crate::path::MarketDataPath;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Granularity {
    Raw,
    Hourly,
    Daily,
    Monthly,
    Yearly,
}

impl Granularity {
    pub const ALL: [Self; 5] = [
        Self::Raw,
        Self::Hourly,
        Self::Daily,
        Self::Monthly,
        Self::Yearly,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Hourly => "hourly",
            Self::Daily => "daily",
            Self::Monthly => "monthly",
            Self::Yearly => "yearly",
        }
    }

    pub const fn source_depth(self) -> usize {
        match self {
            Self::Raw => 4,
            Self::Hourly => 3,
            Self::Daily => 2,
            Self::Monthly => 1,
            Self::Yearly => 0,
        }
    }

    pub const fn target(self) -> Option<Self> {
        match self {
            Self::Raw => Some(Self::Hourly),
            Self::Hourly => Some(Self::Daily),
            Self::Daily => Some(Self::Monthly),
            Self::Monthly => Some(Self::Yearly),
            Self::Yearly => None,
        }
    }

    pub const fn source(self) -> Option<Self> {
        match self {
            Self::Raw => None,
            Self::Hourly => Some(Self::Raw),
            Self::Daily => Some(Self::Hourly),
            Self::Monthly => Some(Self::Daily),
            Self::Yearly => Some(Self::Monthly),
        }
    }

    pub fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "raw" => Some(Self::Raw),
            "hourly" => Some(Self::Hourly),
            "daily" => Some(Self::Daily),
            "monthly" => Some(Self::Monthly),
            "yearly" => Some(Self::Yearly),
            _ => None,
        }
    }

    pub fn from_path(path: &str) -> Option<Self> {
        MarketDataPath::granularity_from_path(path)
    }

    pub fn time_segment_count(path: &str) -> Option<usize> {
        MarketDataPath::time_segment_count(path)
    }
}

impl fmt::Display for Granularity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_require_a_feed_before_the_granularity() {
        let path = "paper/binance/bitcoin/tetherusd/trades/raw/2024/01";
        assert_eq!(Granularity::from_path(path), Some(Granularity::Raw));
        assert_eq!(Granularity::time_segment_count(path), Some(2));
        assert_eq!(
            Granularity::from_path("paper/binance/bitcoin/tetherusd/not-a-feed/raw"),
            None
        );
        assert_eq!(
            Granularity::time_segment_count("paper/binance/bitcoin/tetherusd/not-a-feed/raw"),
            None
        );
    }
}
