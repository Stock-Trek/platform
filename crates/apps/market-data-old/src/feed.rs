use crate::path::MarketDataPath;
use serde::{Deserialize, Serialize};
use std::fmt;

/// The type of market data stored below a trading pair.
///
/// The feed is the path segment between the quote asset and the data
/// granularity, for example `paper/binance/bitcoin/tetherusd/trades/raw/...`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Feed {
    Trades,
    Book,
    Funding,
    OpenInterest,
    Liquidation,
}

impl Feed {
    pub const ALL: [Self; 5] = [
        Self::Trades,
        Self::Book,
        Self::Funding,
        Self::OpenInterest,
        Self::Liquidation,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Book => "book",
            Self::Funding => "funding",
            Self::OpenInterest => "open_interest",
            Self::Liquidation => "liquidation",
        }
    }

    pub fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "trades" => Some(Self::Trades),
            "book" => Some(Self::Book),
            "funding" => Some(Self::Funding),
            "open_interest" => Some(Self::OpenInterest),
            "liquidation" => Some(Self::Liquidation),
            _ => None,
        }
    }

    pub fn from_path(path: &str) -> Option<Self> {
        MarketDataPath::feed_from_path(path)
    }
}

impl fmt::Display for Feed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_segments_round_trip() {
        for feed in Feed::ALL {
            assert_eq!(Feed::from_segment(feed.as_str()), Some(feed));
        }
    }

    #[test]
    fn feed_is_the_segment_after_the_quote_asset() {
        assert_eq!(
            Feed::from_path("paper/binance/bitcoin/tetherusd/open_interest/raw/2024"),
            Some(Feed::OpenInterest)
        );
        assert_eq!(
            Feed::from_path("/paper/binance/bitcoin/tetherusd/book/hourly/"),
            Some(Feed::Book)
        );
        assert_eq!(
            Feed::from_path("paper/binance/bitcoin/tetherusd/not-a-feed/raw"),
            None
        );
    }
}
