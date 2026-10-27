use crate::{
    constants::{PARQUET_SUFFIX, UPDATE_QUANTIZATION},
    granularity::Granularity,
    path::MarketDataPath,
    quantized_time::{Quantization, QuantizedTime, TopOfHour, TopOfMonth},
    s3::ObjectKey,
};
use chrono::{DateTime, TimeZone, Utc};
use platform_errors::BpResult;
use std::collections::HashSet;

/// A single update window that still needs to be ingested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Window {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Window {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }

    pub fn file_path(&self) -> String {
        MarketDataPath::window_file_path(self.start)
    }
}

/// A requested period that was skipped because an existing compacted file covers it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactedSkip {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub compacted_file: String,
    pub windows: usize,
}

/// Windows selected by the planner, including periods it intentionally skipped.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WindowPlan {
    /// Windows that still need to be ingested.
    pub windows: Vec<Window>,
    /// Windows covered by an already compacted period.
    pub windows_compacted: usize,
    /// Details of the compacted periods that were skipped.
    pub skipped_compacted: Vec<CompactedSkip>,
}

/// A complete set of source files that can be rolled up into one compacted file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionTarget {
    pub prefix: ObjectKey,
    pub source_files: Vec<ObjectKey>,
    pub compacted_file: ObjectKey,
}

pub struct Planner;

pub type Plan = Planner;

impl Planner {
    /// Plan the missing windows and report windows covered by compacted periods.
    pub fn plan_windows_with_skipped(
        existing: &HashSet<String>,
        end_time: DateTime<Utc>,
        backfill_start: Option<DateTime<Utc>>,
    ) -> BpResult<WindowPlan> {
        let backfill_start = backfill_start
            .map(|start| QuantizedTime::timestamp(start, UPDATE_QUANTIZATION))
            .transpose()?;
        let existing_start = existing
            .iter()
            .filter_map(|path| Self::file_start_time(path))
            .min()
            .map(|start| QuantizedTime::timestamp(start, UPDATE_QUANTIZATION))
            .transpose()?;
        let start = match (backfill_start, existing_start) {
            (Some(backfill_start), Some(existing_start)) => {
                Some(backfill_start.min(existing_start))
            }
            (Some(backfill_start), None) => Some(backfill_start),
            (None, Some(existing_start)) => Some(existing_start),
            (None, None) => None,
        };
        let Some(start) = start.filter(|start| *start < end_time) else {
            return Ok(WindowPlan {
                windows: vec![Window::new(
                    QuantizedTime::previous_top(end_time, UPDATE_QUANTIZATION)?,
                    end_time,
                )],
                windows_compacted: 0,
                skipped_compacted: Vec::new(),
            });
        };
        Self::plan_windows_with_overwrite(existing, start, end_time, false)
    }

    pub fn plan_windows_with_overwrite(
        existing: &HashSet<String>,
        start: DateTime<Utc>,
        end_time: DateTime<Utc>,
        overwrite_existing: bool,
    ) -> BpResult<WindowPlan> {
        let mut start = QuantizedTime::timestamp(start, UPDATE_QUANTIZATION)?;
        let mut plan = WindowPlan::default();
        while start < end_time {
            if let Some((boundary, compacted_file)) = Self::compacted_boundary(existing, start)? {
                let skipped_end = boundary.min(end_time);
                let windows = Self::window_count(start, skipped_end)?;
                if windows > 0 {
                    plan.windows_compacted += windows;
                    plan.skipped_compacted.push(CompactedSkip {
                        start,
                        end: skipped_end,
                        compacted_file,
                        windows,
                    });
                }
                start = boundary;
                continue;
            }
            let window_end = QuantizedTime::next_top(start, UPDATE_QUANTIZATION)?;
            if window_end > end_time {
                break;
            }
            if overwrite_existing || !existing.contains(&MarketDataPath::window_file_path(start)) {
                plan.windows.push(Window::new(start, window_end));
            }
            start = window_end;
        }
        Ok(plan)
    }

    fn window_count(start: DateTime<Utc>, end: DateTime<Utc>) -> BpResult<usize> {
        let mut count = 0;
        let mut start = start;
        while start < end {
            let window_end = QuantizedTime::next_top(start, UPDATE_QUANTIZATION)?;
            if window_end > end {
                break;
            }
            count += 1;
            start = window_end;
        }
        Ok(count)
    }

    /// Plan compaction for one prefix using a non-default file suffix.
    pub fn plan_compaction_with_suffix<I, K>(
        prefix: &ObjectKey,
        entries: I,
        suffix: &str,
    ) -> BpResult<Option<CompactionTarget>>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let source_files = Self::expected_compaction_sources_with_suffix(prefix, suffix)?;
        if source_files.is_empty() {
            return Ok(None);
        }
        let entries = entries
            .into_iter()
            .map(|entry| entry.as_ref().to_string())
            .collect::<HashSet<_>>();
        if source_files
            .iter()
            .any(|source| !entries.contains(source.as_str()))
        {
            return Ok(None);
        }
        Ok(Some(CompactionTarget {
            prefix: prefix.clone(),
            source_files,
            compacted_file: Self::compacted_file_path(prefix, suffix)?,
        }))
    }

    /// Expected source files for compacting `prefix` with a non-default file suffix.
    pub fn expected_compaction_sources_with_suffix(
        prefix: &ObjectKey,
        suffix: &str,
    ) -> BpResult<Vec<ObjectKey>> {
        MarketDataPath::expected_compaction_sources(prefix, suffix)
    }

    pub fn compacted_file_path(prefix: &ObjectKey, suffix: &str) -> BpResult<ObjectKey> {
        MarketDataPath::compacted_file_path(prefix, suffix)
    }

    fn compacted_boundary(
        existing: &HashSet<String>,
        start_time: DateTime<Utc>,
    ) -> BpResult<Option<(DateTime<Utc>, String)>> {
        let [hour, day, month, year] = MarketDataPath::compacted_file_paths(start_time);
        let (compacted_file, next_period_start) = if existing.contains(&year) {
            (year, Self::next_year(start_time)?)
        } else if existing.contains(&month) {
            (month, Self::next_month(start_time)?)
        } else if existing.contains(&day) {
            (day, Self::next_day(start_time)?)
        } else if existing.contains(&hour) {
            (hour, Self::next_hour(start_time)?)
        } else {
            return Ok(None);
        };
        // The compacted period may end between update windows for coarse
        // quantizations, so resume at the first window at or after its end.
        Ok(Some((
            QuantizedTime::top_at_or_after(next_period_start, UPDATE_QUANTIZATION)?,
            compacted_file,
        )))
    }

    fn file_start_time(path: &str) -> Option<DateTime<Utc>> {
        let path = path.strip_suffix(PARQUET_SUFFIX)?;
        let mut segments = path.split('/').filter(|segment| !segment.is_empty());
        Granularity::from_segment(segments.next()?)?;
        let year = segments.next()?.parse::<i32>().ok()?;
        let month = Self::file_path_segment(segments.next(), 1)?;
        let day = Self::file_path_segment(segments.next(), 1)?;
        let hour = Self::file_path_segment(segments.next(), 0)?;
        let minute = Self::file_path_segment(segments.next(), 0)?;
        if segments.next().is_some() {
            return None;
        }
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
    }

    fn file_path_segment(segment: Option<&str>, default: u32) -> Option<u32> {
        match segment {
            Some(segment) => segment.parse().ok(),
            None => Some(default),
        }
    }

    fn next_hour(timestamp: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        let hour_start = QuantizedTime::timestamp(timestamp, Quantization::Hour(TopOfHour::One))?;
        QuantizedTime::next_top(hour_start, Quantization::Hour(TopOfHour::One))
    }

    fn next_day(timestamp: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        let day_start = QuantizedTime::timestamp(timestamp, Quantization::Day)?;
        QuantizedTime::next_top(day_start, Quantization::Day)
    }

    fn next_month(timestamp: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        let month_start =
            QuantizedTime::timestamp(timestamp, Quantization::Month(TopOfMonth::One))?;
        QuantizedTime::next_top(month_start, Quantization::Month(TopOfMonth::One))
    }

    fn next_year(timestamp: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        let year_start = QuantizedTime::timestamp(timestamp, Quantization::Year)?;
        QuantizedTime::next_top(year_start, Quantization::Year)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::PARQUET_SUFFIX;
    use chrono::{DateTime, TimeZone, Utc};

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .unwrap()
    }

    fn paths(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|path| (*path).to_string()).collect()
    }

    fn window(start: DateTime<Utc>, end: DateTime<Utc>) -> Window {
        Window::new(start, end)
    }

    #[test]
    fn no_existing_files_plans_only_the_latest_window() {
        let end = utc(2024, 1, 1, 12, 0);
        assert_eq!(
            Planner::plan_windows_with_skipped(&HashSet::new(), end, None)
                .unwrap()
                .windows,
            vec![window(utc(2024, 1, 1, 11, 54), end)]
        );
    }

    #[test]
    fn backfill_plans_missing_windows_from_the_requested_start() {
        let end = utc(2024, 1, 1, 0, 18);
        assert_eq!(
            Planner::plan_windows_with_skipped(&HashSet::new(), end, Some(utc(2024, 1, 1, 0, 0)))
                .unwrap()
                .windows,
            vec![
                window(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)),
                window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12)),
                window(utc(2024, 1, 1, 0, 12), utc(2024, 1, 1, 0, 18)),
            ]
        );
    }

    #[test]
    fn existing_windows_are_skipped_and_the_earliest_existing_sets_the_start() {
        let existing = paths(&[
            "raw/2024/01/01/00/00.parquet",
            "raw/2024/01/01/00/12.parquet",
        ]);
        assert_eq!(
            Planner::plan_windows_with_skipped(&existing, utc(2024, 1, 1, 0, 18), None)
                .unwrap()
                .windows,
            vec![window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12))]
        );
    }

    #[test]
    fn backfill_can_start_before_the_earliest_existing_window() {
        let existing = paths(&["raw/2024/01/01/00/12.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_skipped(
                &existing,
                utc(2024, 1, 1, 0, 18),
                Some(utc(2024, 1, 1, 0, 0))
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)),
                window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12)),
            ]
        );
    }

    #[test]
    fn plan_windows_between_quantizes_the_start() {
        let existing = paths(&["raw/2024/01/01/00/00.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_overwrite(
                &existing,
                utc(2024, 1, 1, 0, 2),
                utc(2024, 1, 1, 0, 12),
                false
            )
            .unwrap()
            .windows,
            vec![window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12))]
        );
    }

    #[test]
    fn forced_backfill_replans_existing_windows() {
        let existing = paths(&[
            "raw/2024/01/01/00/00.parquet",
            "raw/2024/01/01/00/06.parquet",
        ]);
        assert_eq!(
            Planner::plan_windows_with_overwrite(
                &existing,
                utc(2024, 1, 1, 0, 2),
                utc(2024, 1, 1, 0, 12),
                true,
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2024, 1, 1, 0, 0), utc(2024, 1, 1, 0, 6)),
                window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12)),
            ]
        );
    }

    #[test]
    fn non_forced_backfill_skips_existing_windows() {
        let existing = paths(&["raw/2024/01/01/00/00.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_overwrite(
                &existing,
                utc(2024, 1, 1, 0, 0),
                utc(2024, 1, 1, 0, 12),
                false,
            )
            .unwrap()
            .windows,
            vec![window(utc(2024, 1, 1, 0, 6), utc(2024, 1, 1, 0, 12))]
        );
    }

    #[test]
    fn forced_backfill_still_skips_compacted_periods() {
        let existing = paths(&["hourly/2024/01/01/00.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_overwrite(
                &existing,
                utc(2024, 1, 1, 0, 0),
                utc(2024, 1, 1, 1, 12),
                true,
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2024, 1, 1, 1, 0), utc(2024, 1, 1, 1, 6)),
                window(utc(2024, 1, 1, 1, 6), utc(2024, 1, 1, 1, 12)),
            ]
        );
    }

    #[test]
    fn forced_backfill_reports_compacted_periods() {
        let existing = paths(&["hourly/2024/01/01/00.parquet"]);
        let plan = Planner::plan_windows_with_overwrite(
            &existing,
            utc(2024, 1, 1, 0, 0),
            utc(2024, 1, 1, 1, 12),
            true,
        )
        .unwrap();
        assert_eq!(
            plan.windows,
            vec![
                window(utc(2024, 1, 1, 1, 0), utc(2024, 1, 1, 1, 6)),
                window(utc(2024, 1, 1, 1, 6), utc(2024, 1, 1, 1, 12)),
            ]
        );
        assert_eq!(plan.windows_compacted, 10);
        assert_eq!(
            plan.skipped_compacted,
            vec![CompactedSkip {
                start: utc(2024, 1, 1, 0, 0),
                end: utc(2024, 1, 1, 1, 0),
                compacted_file: "hourly/2024/01/01/00.parquet".to_string(),
                windows: 10,
            }]
        );
    }

    #[test]
    fn compacted_skips_are_clipped_to_the_requested_range() {
        let existing = paths(&["hourly/2024/01/01/00.parquet"]);
        let plan = Planner::plan_windows_with_overwrite(
            &existing,
            utc(2024, 1, 1, 0, 0),
            utc(2024, 1, 1, 0, 30),
            true,
        )
        .unwrap();
        assert!(plan.windows.is_empty());
        assert_eq!(plan.windows_compacted, 5);
        assert_eq!(plan.skipped_compacted[0].end, utc(2024, 1, 1, 0, 30));
    }

    #[test]
    fn plain_windows_plan_reports_compacted_periods() {
        let existing = paths(&["hourly/2024/01/01/00.parquet"]);
        let plan = Planner::plan_windows_with_overwrite(
            &existing,
            utc(2024, 1, 1, 0, 0),
            utc(2024, 1, 1, 0, 12),
            false,
        )
        .unwrap();
        assert!(plan.windows.is_empty());
        assert_eq!(plan.windows_compacted, 2);
    }

    #[test]
    fn fully_ingested_latest_window_has_no_work() {
        let existing = paths(&["raw/2024/01/01/00/00.parquet"]);
        assert!(
            Planner::plan_windows_with_skipped(&existing, utc(2024, 1, 1, 0, 6), None)
                .unwrap()
                .windows
                .is_empty()
        );
    }

    #[test]
    fn compacted_hours_are_skipped() {
        let existing = paths(&["hourly/2024/01/01/03.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_skipped(
                &existing,
                utc(2024, 1, 1, 4, 12),
                Some(utc(2024, 1, 1, 3, 0))
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2024, 1, 1, 4, 0), utc(2024, 1, 1, 4, 6)),
                window(utc(2024, 1, 1, 4, 6), utc(2024, 1, 1, 4, 12)),
            ]
        );
    }

    #[test]
    fn compacted_windows_are_reported_as_compacted() {
        let existing = paths(&["hourly/2024/01/01/03.parquet"]);
        let plan = Planner::plan_windows_with_skipped(
            &existing,
            utc(2024, 1, 1, 4, 12),
            Some(utc(2024, 1, 1, 3, 0)),
        )
        .unwrap();
        assert_eq!(
            plan.windows,
            vec![
                window(utc(2024, 1, 1, 4, 0), utc(2024, 1, 1, 4, 6)),
                window(utc(2024, 1, 1, 4, 6), utc(2024, 1, 1, 4, 12)),
            ]
        );
        assert_eq!(plan.windows_compacted, 10);
    }

    #[test]
    fn forced_backfill_reports_compacted_windows_as_compacted() {
        let existing = paths(&["hourly/2024/01/01/00.parquet"]);
        let plan = Planner::plan_windows_with_overwrite(
            &existing,
            utc(2024, 1, 1, 0, 0),
            utc(2024, 1, 1, 1, 12),
            true,
        )
        .unwrap();
        assert_eq!(
            plan.windows,
            vec![
                window(utc(2024, 1, 1, 1, 0), utc(2024, 1, 1, 1, 6)),
                window(utc(2024, 1, 1, 1, 6), utc(2024, 1, 1, 1, 12)),
            ]
        );
        assert_eq!(plan.windows_compacted, 10);
    }

    #[test]
    fn compacted_days_are_skipped() {
        let existing = paths(&["daily/2024/01/01.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_skipped(
                &existing,
                utc(2024, 1, 2, 0, 12),
                Some(utc(2024, 1, 1, 12, 0))
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2024, 1, 2, 0, 0), utc(2024, 1, 2, 0, 6)),
                window(utc(2024, 1, 2, 0, 6), utc(2024, 1, 2, 0, 12)),
            ]
        );
    }

    #[test]
    fn compacted_months_are_skipped() {
        let existing = paths(&["monthly/2024/01.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_skipped(
                &existing,
                utc(2024, 2, 1, 0, 12),
                Some(utc(2024, 1, 15, 0, 0))
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2024, 2, 1, 0, 0), utc(2024, 2, 1, 0, 6)),
                window(utc(2024, 2, 1, 0, 6), utc(2024, 2, 1, 0, 12)),
            ]
        );
    }

    #[test]
    fn compacted_years_are_skipped() {
        let existing = paths(&["yearly/2024.parquet"]);
        assert_eq!(
            Planner::plan_windows_with_skipped(
                &existing,
                utc(2025, 1, 1, 0, 12),
                Some(utc(2024, 7, 1, 0, 0))
            )
            .unwrap()
            .windows,
            vec![
                window(utc(2025, 1, 1, 0, 0), utc(2025, 1, 1, 0, 6)),
                window(utc(2025, 1, 1, 0, 6), utc(2025, 1, 1, 0, 12)),
            ]
        );
    }

    #[test]
    fn complete_hour_produces_a_compaction_target() {
        let prefix = ObjectKey::new("paper/binance/bitcoin/tether_usd/trades/raw/2024/01/01/00/");
        let source_files = (0..60)
            .step_by(6)
            .map(|minute| ObjectKey::new(format!("{prefix}{minute:02}.parquet")))
            .collect::<Vec<_>>();
        let target = Planner::plan_compaction_with_suffix(&prefix, &source_files, PARQUET_SUFFIX)
            .unwrap()
            .unwrap();
        assert_eq!(
            target,
            CompactionTarget {
                prefix: prefix.clone(),
                source_files,
                compacted_file: ObjectKey::new(
                    "paper/binance/bitcoin/tether_usd/trades/hourly/2024/01/01/00.parquet"
                ),
            }
        );
    }

    #[test]
    fn complete_day_produces_a_compaction_target() {
        let prefix = ObjectKey::new("paper/binance/bitcoin/tether_usd/trades/hourly/2024/01/01/");
        let source_files = (0..24)
            .map(|hour| ObjectKey::new(format!("{prefix}{hour:02}.parquet")))
            .collect::<Vec<_>>();
        let target = Planner::plan_compaction_with_suffix(&prefix, &source_files, PARQUET_SUFFIX)
            .unwrap()
            .unwrap();
        assert_eq!(
            target.compacted_file,
            ObjectKey::new("paper/binance/bitcoin/tether_usd/trades/daily/2024/01/01.parquet")
        );
        assert_eq!(target.source_files, source_files);
    }

    #[test]
    fn missing_source_file_does_not_produce_a_target() {
        let prefix = ObjectKey::new("paper/binance/bitcoin/tether_usd/trades/raw/2024/01/01/00/");
        let mut source_files = (0..60)
            .step_by(6)
            .map(|minute| ObjectKey::new(format!("{prefix}{minute:02}.parquet")))
            .collect::<Vec<_>>();
        source_files.pop();
        assert_eq!(
            Planner::plan_compaction_with_suffix(&prefix, &source_files, PARQUET_SUFFIX).unwrap(),
            None
        );
    }

    #[test]
    fn leaf_path_does_not_produce_a_target() {
        let prefix =
            ObjectKey::new("paper/binance/bitcoin/tether_usd/trades/raw/2024/01/01/00/00/");
        assert_eq!(
            Planner::plan_compaction_with_suffix(&prefix, [] as [ObjectKey; 0], PARQUET_SUFFIX)
                .unwrap(),
            None
        );
    }
}
