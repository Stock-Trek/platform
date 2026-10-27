use crate::{
    constants::PARQUET_SUFFIX, feed::Feed, granularity::Granularity, s3::ObjectKey,
    subfolders::SubFolders,
};
use chrono::{DateTime, Datelike, Timelike, Utc};
use platform_errors::{BpError, BpResult, DataError};
use std::fmt;

/// The root path segment that contains in-progress staged files.
pub const STAGING_SEGMENT: &str = "staging";
/// The path segment that identifies a market data feed.
pub const FEED_SEGMENT: usize = 4;
/// The path segment that identifies a market data tier.
pub const TIER_SEGMENT: usize = 5;
/// The first path segment that contains time information.
pub const TIME_SEGMENT_START: usize = 6;

/// The single place where market data paths are built and interpreted.
///
/// The layout is `mode/cex/base/quote/feed/tier/time...`. Keeping the layout
/// here means callers never need to know which segment holds which value or how
/// a path is joined together.
pub struct MarketDataPath;

impl MarketDataPath {
    pub const FEED_SEGMENT: usize = crate::path::FEED_SEGMENT;
    pub const TIER_SEGMENT: usize = crate::path::TIER_SEGMENT;
    pub const TIME_SEGMENT_START: usize = crate::path::TIME_SEGMENT_START;

    /// Split a market data path into its non-empty segments.
    pub fn segments(path: &str) -> Vec<&str> {
        path.split('/')
            .filter(|segment| !segment.is_empty())
            .collect()
    }

    /// Build the trading-pair portion of a path.
    pub fn trading_pair(
        trading_mode: impl fmt::Display,
        cex_id: impl fmt::Display,
        base: impl fmt::Display,
        quote: impl fmt::Display,
    ) -> String {
        format!("{trading_mode}/{cex_id}/{base}/{quote}")
    }

    /// Build a trading-pair prefix with a trailing slash.
    pub fn trading_pair_prefix(
        trading_mode: impl fmt::Display,
        cex_id: impl fmt::Display,
        base: impl fmt::Display,
        quote: impl fmt::Display,
    ) -> ObjectKey {
        ObjectKey::new(format!(
            "{}/",
            Self::trading_pair(trading_mode, cex_id, base, quote)
        ))
    }

    /// Build the feed portion of a path.
    pub fn feed(pair: &str, feed: Feed) -> String {
        Self::join(pair, feed.as_str())
    }

    /// Build a feed prefix with a trailing slash.
    pub fn feed_prefix(pair: &str, feed: Feed) -> ObjectKey {
        ObjectKey::new(format!("{}/", Self::feed(pair, feed)))
    }

    /// Build a tier prefix from a trading-pair path.
    pub fn tier_prefix(pair: &str, feed: Feed, tier: Granularity) -> ObjectKey {
        ObjectKey::new(format!(
            "{}/{}/{}/",
            pair.trim_end_matches('/'),
            feed.as_str(),
            tier.as_str()
        ))
    }

    /// Join a path prefix and child, normalizing the separator between them.
    ///
    /// A trailing slash on the child is preserved, so this can build both
    /// folder prefixes and file paths.
    pub fn join(prefix: &str, child: &str) -> String {
        let prefix = prefix.trim_end_matches('/');
        let child = child.trim_start_matches('/');
        match (prefix.is_empty(), child.is_empty()) {
            (true, _) => child.to_string(),
            (false, true) => format!("{prefix}/"),
            (false, false) => format!("{prefix}/{child}"),
        }
    }

    /// Join a path prefix and child into an S3 object key.
    pub fn join_key(prefix: &str, child: &str) -> ObjectKey {
        ObjectKey::new(Self::join(prefix, child))
    }

    /// Append a suffix, such as a Parquet extension, to a path.
    pub fn with_suffix(path: &str, suffix: &str) -> String {
        format!("{path}{suffix}")
    }

    /// Build the relative path of the raw file for an update window.
    pub fn window_file_path(start_time: DateTime<Utc>) -> String {
        Self::tier_file(Granularity::Raw, start_time)
    }

    /// Build the object key of the raw file for an update window.
    pub fn window_file(prefix: &str, start_time: DateTime<Utc>) -> ObjectKey {
        Self::join_key(prefix, &Self::window_file_path(start_time))
    }

    /// Build a file path for a tier from a timestamp.
    pub fn tier_file(tier: Granularity, start_time: DateTime<Utc>) -> String {
        let year = start_time.year();
        let month = start_time.month() as i32;
        let day = start_time.day() as i32;
        let hour = start_time.hour() as i32;
        let minute = start_time.minute() as i32;
        let time_segments = match tier {
            Granularity::Raw => vec![year, month, day, hour, minute],
            Granularity::Hourly => vec![year, month, day, hour],
            Granularity::Daily => vec![year, month, day],
            Granularity::Monthly => vec![year, month],
            Granularity::Yearly => vec![year],
        };
        format!("{}/{}", tier, Self::file(&time_segments, PARQUET_SUFFIX))
    }

    /// Build the paths of the compacted files that cover a timestamp.
    pub fn compacted_file_paths(start_time: DateTime<Utc>) -> [String; 4] {
        [
            Self::tier_file(Granularity::Hourly, start_time),
            Self::tier_file(Granularity::Daily, start_time),
            Self::tier_file(Granularity::Monthly, start_time),
            Self::tier_file(Granularity::Yearly, start_time),
        ]
    }

    /// Build the compacted file path for a source prefix.
    pub fn compacted_file_path(prefix: &ObjectKey, suffix: &str) -> BpResult<ObjectKey> {
        let path = prefix.as_str().trim_end_matches('/');
        let segments = Self::segments(path);
        let tier = segments
            .get(TIER_SEGMENT)
            .and_then(|segment| Granularity::from_segment(segment))
            .ok_or_else(|| {
                BpError::Data(DataError::InvalidJob {
                    message: format!("compaction path {path} must include a data tier"),
                })
            })?;
        if segments
            .get(FEED_SEGMENT)
            .and_then(|segment| Feed::from_segment(segment))
            .is_none()
        {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compaction path {path} must include a data feed"),
            }));
        }
        let target = tier.target().ok_or_else(|| {
            BpError::Data(DataError::InvalidJob {
                message: format!("the {tier} tier has no compaction target"),
            })
        })?;
        let mut target_path = segments[..TIER_SEGMENT].join("/");
        target_path.push('/');
        target_path.push_str(target.as_str());
        for segment in &segments[TIME_SEGMENT_START..] {
            target_path.push('/');
            target_path.push_str(segment);
        }
        Ok(ObjectKey::new(Self::with_suffix(&target_path, suffix)))
    }

    /// Build the expected source file keys for compacting a prefix.
    pub fn expected_compaction_sources(
        prefix: &ObjectKey,
        suffix: &str,
    ) -> BpResult<Vec<ObjectKey>> {
        let expected_subfolders = SubFolders::subfolders_from_path(prefix.as_str())?;
        Ok(expected_subfolders
            .iter()
            .map(|subfolder| {
                let subfolder = subfolder.strip_suffix('/').unwrap_or(subfolder);
                Self::join_key(prefix.as_str(), &Self::with_suffix(subfolder, suffix))
            })
            .collect())
    }

    /// Replace the tier segment of a prefix.
    pub fn replace_tier(prefix: &ObjectKey, tier: Granularity) -> BpResult<ObjectKey> {
        let mut segments = Self::segments(prefix.as_str())
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let Some(segment) = segments.get_mut(TIER_SEGMENT) else {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compaction prefix {prefix} must include a data feed and tier"),
            }));
        };
        *segment = tier.as_str().to_string();
        Ok(ObjectKey::new(format!("{}/", segments.join("/"))))
    }

    /// Build the staging key for a compacted file.
    ///
    /// Staged files live under a dedicated root segment so they cannot be
    /// mistaken for completed market data files.
    pub fn staged_file_path(compacted_file: &ObjectKey) -> ObjectKey {
        Self::join_key(STAGING_SEGMENT, compacted_file.as_str())
    }

    /// Normalize and validate a requested compaction target.
    pub fn compact_prefix(target: &str) -> BpResult<ObjectKey> {
        let target = target.trim();
        if target.is_empty() {
            return Err(BpError::Data(DataError::InvalidJob {
                message: "compact target cannot be empty".to_string(),
            }));
        }
        let is_file_target = !target.ends_with('/');
        let target = target.trim_end_matches('/');
        let segments = target.split('/').collect::<Vec<_>>();
        if segments
            .iter()
            .any(|segment| segment.is_empty() || matches!(*segment, "." | ".."))
        {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compact target {target} contains an invalid path segment"),
            }));
        }
        let directory_segments = if is_file_target
            && segments
                .last()
                .is_some_and(|segment| segment.ends_with(PARQUET_SUFFIX))
        {
            &segments[..segments.len() - 1]
        } else {
            &segments[..]
        };
        // The data layout is mode/cex/base/quote/feed, followed by a required
        // tier and up to four time segments (year/month/day/hour). A file target
        // may additionally name the leaf, but its parent must still be a valid
        // directory depth.
        if !(TIME_SEGMENT_START..=TIME_SEGMENT_START + 4).contains(&directory_segments.len()) {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!(
                    "compact target {target} must address a feed, data tier, or one of its year, month, day, or hour folders"
                ),
            }));
        }
        if directory_segments
            .get(FEED_SEGMENT)
            .and_then(|segment| Feed::from_segment(segment))
            .is_none()
        {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compact target {target} must include a data feed"),
            }));
        }
        if directory_segments
            .get(TIER_SEGMENT)
            .and_then(|segment| Granularity::from_segment(segment))
            .is_none()
        {
            return Err(BpError::Data(DataError::InvalidJob {
                message: format!("compact target {target} must include a data tier"),
            }));
        }
        let pair = Self::trading_pair(
            directory_segments[0],
            directory_segments[1],
            directory_segments[2],
            directory_segments[3],
        );
        crate::job::TradingPairPath::new(pair).resolve()?;
        let prefix = format!("{}/", directory_segments.join("/"));
        SubFolders::subfolders_from_path(&prefix).map_err(|error| {
            BpError::Data(DataError::InvalidJob {
                message: format!("compact target {target} has an invalid time path: {error}"),
            })
        })?;
        Ok(ObjectKey::new(prefix))
    }

    /// Build a zero-padded time file path.
    pub fn file(numbers: &[i32], suffix: &str) -> String {
        let path = numbers
            .iter()
            .map(|number| format!("{number:02}"))
            .collect::<Vec<_>>()
            .join("/");
        Self::with_suffix(&path, suffix)
    }

    /// Build zero-padded child folder names with trailing slashes.
    pub fn subfolders(numbers: &[i32]) -> Vec<String> {
        numbers
            .iter()
            .map(|number| format!("{number:02}/"))
            .collect()
    }

    /// Read the feed segment from a path.
    pub fn feed_from_path(path: &str) -> Option<Feed> {
        Self::segments(path)
            .get(FEED_SEGMENT)
            .and_then(|segment| Feed::from_segment(segment))
    }

    /// Read the tier segment from a path, after validating the feed segment.
    pub fn granularity_from_path(path: &str) -> Option<Granularity> {
        let segments = Self::segments(path);
        Feed::from_segment(segments.get(FEED_SEGMENT)?)?;
        Granularity::from_segment(segments.get(TIER_SEGMENT)?)
    }

    /// Count the time segments after the tier, after validating the path prefix.
    pub fn time_segment_count(path: &str) -> Option<usize> {
        let segments = Self::segments(path);
        Feed::from_segment(segments.get(FEED_SEGMENT)?)?;
        Granularity::from_segment(segments.get(TIER_SEGMENT)?)?;
        Some(segments.len() - TIME_SEGMENT_START)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use stock_trek_types::cex::asset_id::AssetId;

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
    }

    #[test]
    fn builds_the_complete_path_from_its_parts() {
        let pair = MarketDataPath::trading_pair("paper", "binance", "bitcoin", "tether_usd");
        assert_eq!(pair, "paper/binance/bitcoin/tether_usd");
        assert_eq!(
            MarketDataPath::feed_prefix(&pair, Feed::Trades).as_str(),
            "paper/binance/bitcoin/tether_usd/trades/"
        );
        assert_eq!(
            MarketDataPath::tier_prefix(&pair, Feed::Trades, Granularity::Raw).as_str(),
            "paper/binance/bitcoin/tether_usd/trades/raw/"
        );
    }

    #[test]
    fn builds_asset_segments_with_the_established_path_spelling() {
        assert_eq!(AssetId::Bitcoin.to_string(), "bitcoin".to_string());
        assert_eq!(AssetId::TetherUSD.to_string(), "tether_usd".to_string());
    }

    #[test]
    fn joins_prefixes_and_children_without_duplicate_separators() {
        assert_eq!(
            MarketDataPath::join("paper/binance/bitcoin/tether_usd/trades/raw/", "00/"),
            "paper/binance/bitcoin/tether_usd/trades/raw/00/"
        );
        assert_eq!(
            MarketDataPath::join(
                "/paper/binance/bitcoin/tether_usd/trades/raw",
                "/00.parquet"
            ),
            "/paper/binance/bitcoin/tether_usd/trades/raw/00.parquet"
        );
    }

    #[test]
    fn builds_window_and_compacted_file_paths() {
        let start = utc(2024, 1, 1, 3, 6);
        assert_eq!(
            MarketDataPath::window_file_path(start),
            "raw/2024/01/01/03/06.parquet"
        );
        assert_eq!(
            MarketDataPath::tier_file(Granularity::Hourly, start),
            "hourly/2024/01/01/03.parquet"
        );
        assert_eq!(
            MarketDataPath::compacted_file_paths(start),
            [
                "hourly/2024/01/01/03.parquet",
                "daily/2024/01/01.parquet",
                "monthly/2024/01.parquet",
                "yearly/2024.parquet",
            ]
        );
    }

    #[test]
    fn places_staged_files_under_the_staging_root_segment() {
        let compacted_file =
            ObjectKey::new("paper/binance/bitcoin/tether_usd/trades/hourly/2024/01/01/03.parquet");
        assert_eq!(
            MarketDataPath::staged_file_path(&compacted_file).as_str(),
            "staging/paper/binance/bitcoin/tether_usd/trades/hourly/2024/01/01/03.parquet"
        );
    }

    #[test]
    fn parses_feed_and_granularity_from_the_same_layout() {
        let path = "paper/binance/bitcoin/tether_usd/open_interest/hourly/2024/01";
        assert_eq!(
            MarketDataPath::feed_from_path(path),
            Some(Feed::OpenInterest)
        );
        assert_eq!(
            MarketDataPath::granularity_from_path(path),
            Some(Granularity::Hourly)
        );
        assert_eq!(MarketDataPath::time_segment_count(path), Some(2));
        assert_eq!(
            MarketDataPath::feed_from_path("paper/binance/bitcoin/tether_usd/not-a-feed/raw"),
            None
        );
    }

    #[test]
    fn normalizes_compaction_targets() {
        assert_eq!(
            MarketDataPath::compact_prefix("paper/binance/bitcoin/tether_usd/trades/raw")
                .unwrap()
                .as_str(),
            "paper/binance/bitcoin/tether_usd/trades/raw/"
        );
        assert_eq!(
            MarketDataPath::compact_prefix(
                "paper/binance/bitcoin/tether_usd/trades/raw/2024/01/01/00/06.parquet"
            )
            .unwrap()
            .as_str(),
            "paper/binance/bitcoin/tether_usd/trades/raw/2024/01/01/00/"
        );
    }
}
