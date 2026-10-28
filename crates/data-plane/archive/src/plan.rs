use crate::{
    constants::UPDATE_QUANTIZATION,
    quantized_time::{Quantization, QuantizedTime},
    s3::ObjectKey,
};
use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use std::collections::HashSet;

/// A single update window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Window {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Window {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }
}

/// A complete set of source files that can be rolled up into one compacted file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionTarget {
    pub prefix: ObjectKey,
    pub source_files: Vec<ObjectKey>,
    pub compacted_file: ObjectKey,
}

/// The result of checking whether a prefix is ready to compact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompactionDecision {
    /// Every expected source file exists and no compacted output exists yet.
    Compact(CompactionTarget),
    /// The compacted output already exists, so the period can be skipped.
    AlreadyCompacted { compacted_file: ObjectKey },
    /// One or more expected source files are missing.
    Incomplete { missing: Vec<ObjectKey> },
}

pub struct Planner;

pub type Plan = Planner;

impl Planner {
    /// Plan every complete update window between `start` and `end`.
    ///
    /// `start` is quantized down to the beginning of its window, matching the
    /// window start used for object keys. A trailing partial window is omitted.
    pub fn plan_windows(start: DateTime<Utc>, end: DateTime<Utc>) -> BpResult<Vec<Window>> {
        Self::plan_missing_windows(&HashSet::new(), start, end, true)
    }

    /// Plan the update windows that are not already present in `existing`.
    ///
    /// The values in `existing` are window starts. When `overwrite_existing` is
    /// true, every complete window is planned regardless of the existing set.
    pub fn plan_missing_windows(
        existing: &HashSet<DateTime<Utc>>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        overwrite_existing: bool,
    ) -> BpResult<Vec<Window>> {
        let mut start = QuantizedTime::timestamp(start, UPDATE_QUANTIZATION)?;
        let mut windows = Vec::new();
        while start < end {
            let window_end = QuantizedTime::next_top(start, UPDATE_QUANTIZATION)?;
            if window_end > end {
                break;
            }
            if overwrite_existing || !existing.contains(&start) {
                windows.push(Window::new(start, window_end));
            }
            start = window_end;
        }
        Ok(windows)
    }

    /// Count complete update windows between `start` and `end`.
    pub fn window_count(start: DateTime<Utc>, end: DateTime<Utc>) -> BpResult<usize> {
        Ok(Self::plan_windows(start, end)?.len())
    }

    /// Decide whether `prefix` is ready to compact.
    ///
    /// Compaction is skipped when `compacted_file` already exists. Otherwise it
    /// is only planned when every expected source file is present.
    pub fn plan_compaction(
        prefix: &ObjectKey,
        expected_sources: &[ObjectKey],
        compacted_file: &ObjectKey,
        existing: &HashSet<String>,
    ) -> CompactionDecision {
        if existing.contains(compacted_file.as_str()) {
            return CompactionDecision::AlreadyCompacted {
                compacted_file: compacted_file.clone(),
            };
        }
        let missing = expected_sources
            .iter()
            .filter(|source| !existing.contains(source.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() || expected_sources.is_empty() {
            return CompactionDecision::Incomplete { missing };
        }
        CompactionDecision::Compact(CompactionTarget {
            prefix: prefix.clone(),
            source_files: expected_sources.to_vec(),
            compacted_file: compacted_file.clone(),
        })
    }

    /// Quantization used by update-window planning.
    pub const fn update_quantization() -> Quantization {
        UPDATE_QUANTIZATION
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
    }

    fn window(start: DateTime<Utc>, end: DateTime<Utc>) -> Window {
        Window::new(start, end)
    }

    fn existing(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|key| (*key).to_string()).collect()
    }

    #[test]
    fn plans_every_complete_window_in_a_range() {
        assert_eq!(
            Planner::plan_windows(utc(2024, 1, 1, 0, 2), utc(2024, 1, 1, 0, 18)).unwrap(),
            vec![
                window(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)),
                window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12)),
                window(utc(2024, 1, 1, 0, 12), utc(2024, 1, 1, 0, 18)),
            ]
        );
    }

    #[test]
    fn quantizes_the_start_down_to_its_window() {
        assert_eq!(
            Planner::plan_windows(utc(2024, 1, 1, 0, 7), utc(2024, 1, 1, 0, 12)).unwrap(),
            vec![window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12))]
        );
    }

    #[test]
    fn omits_a_trailing_partial_window() {
        assert_eq!(
            Planner::plan_windows(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 7)).unwrap(),
            vec![window(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6))]
        );
    }

    #[test]
    fn existing_windows_are_skipped() {
        let existing = [utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)]
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(
            Planner::plan_missing_windows(
                &existing,
                utc(2024, 1, 1, 0, 0),
                utc(2024, 1, 1, 0, 18),
                false,
            )
            .unwrap(),
            vec![window(utc(2024, 1, 1, 0, 12), utc(2024, 1, 1, 0, 18))]
        );
    }

    #[test]
    fn forced_planning_replans_existing_windows() {
        let existing = [utc(2024, 1, 1, 0, 0)].into_iter().collect::<HashSet<_>>();
        assert_eq!(
            Planner::plan_missing_windows(
                &existing,
                utc(2024, 1, 1, 0, 2),
                utc(2024, 1, 1, 0, 12),
                true,
            )
            .unwrap(),
            vec![
                window(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)),
                window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12)),
            ]
        );
    }

    #[test]
    fn complete_source_set_produces_a_compaction_target() {
        let prefix = ObjectKey::new("market-data/raw/events/year=2024/month=01/day=01/hour=00/");
        let source_files = vec![
            ObjectKey::new(format!("{prefix}00.parquet")),
            ObjectKey::new(format!("{prefix}06.parquet")),
        ];
        let compacted_file = ObjectKey::new(
            "market-data/compacted/kind=events/exchange=binance/stream=btc_usdt@trade/year=2024/month=01/day=01/hour=00/data.parquet",
        );
        let existing = existing(&[
            "market-data/raw/events/year=2024/month=01/day=01/hour=00/00.parquet",
            "market-data/raw/events/year=2024/month=01/day=01/hour=00/06.parquet",
        ]);
        assert_eq!(
            Planner::plan_compaction(&prefix, &source_files, &compacted_file, &existing),
            CompactionDecision::Compact(CompactionTarget {
                prefix,
                source_files,
                compacted_file,
            })
        );
    }

    #[test]
    fn missing_source_does_not_produce_a_target() {
        let prefix = ObjectKey::new("market-data/raw/events/year=2024/month=01/day=01/hour=00/");
        let source_files = vec![
            ObjectKey::new(format!("{prefix}00.parquet")),
            ObjectKey::new(format!("{prefix}06.parquet")),
        ];
        let compacted_file = ObjectKey::new(
            "market-data/compacted/kind=events/exchange=binance/stream=btc_usdt@trade/year=2024/month=01/day=01/hour=00/data.parquet",
        );
        let existing =
            existing(&["market-data/raw/events/year=2024/month=01/day=01/hour=00/00.parquet"]);
        assert_eq!(
            Planner::plan_compaction(&prefix, &source_files, &compacted_file, &existing),
            CompactionDecision::Incomplete {
                missing: vec![ObjectKey::new(format!("{prefix}06.parquet"))],
            }
        );
    }

    #[test]
    fn already_compacted_periods_are_skipped() {
        let prefix = ObjectKey::new("market-data/raw/events/year=2024/month=01/day=01/hour=00/");
        let source_files = vec![ObjectKey::new(format!("{prefix}00.parquet"))];
        let compacted_file = ObjectKey::new(
            "market-data/compacted/kind=events/exchange=binance/stream=btc_usdt@trade/year=2024/month=01/day=01/hour=00/data.parquet",
        );
        let existing = existing(&[compacted_file.as_str()]);
        assert_eq!(
            Planner::plan_compaction(&prefix, &source_files, &compacted_file, &existing),
            CompactionDecision::AlreadyCompacted {
                compacted_file: compacted_file.clone(),
            }
        );
    }
}
