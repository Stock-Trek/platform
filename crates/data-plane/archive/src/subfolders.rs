use crate::{constants::UPDATE_QUANTIZATION, quantized_time::Quantization};
use chrono::{Datelike, NaiveDate};
use platform_errors::{BpError, BpResult, DataError};

const YEAR_FIELD: &str = "year=";
const MONTH_FIELD: &str = "month=";
const DAY_FIELD: &str = "day=";
const HOUR_FIELD: &str = "hour=";
const TIME_FIELDS: [&str; 4] = [YEAR_FIELD, MONTH_FIELD, DAY_FIELD, HOUR_FIELD];

pub struct SubFolders;

impl SubFolders {
    /// Expected child folders for compaction, using the configured update quantization.
    pub fn subfolders_from_path(path: &str) -> BpResult<Vec<String>> {
        Self::subfolders_from_path_with_quantization(path, UPDATE_QUANTIZATION)
    }

    /// Expected child folders at `path` for a given quantization.
    ///
    /// Paths use the archive layout's `year=.../month=.../day=.../hour=...`
    /// fields. The update path layout stores each window under the start of that
    /// window, so the expected children are the window starts at the next path
    /// level. Archive files live directly below the hour field, so a complete
    /// hour has no expected child folders.
    pub fn subfolders_from_path_with_quantization(
        path: &str,
        quantization: Quantization,
    ) -> BpResult<Vec<String>> {
        let time = TimeFields::parse(path)?;
        if time.year.is_none() {
            return Err(BpError::Data(DataError::InvalidYearMonth {
                path: path.to_string(),
            }));
        }

        if time.month.is_none() {
            Self::parse_year(time.year.unwrap(), path)?;
            return Ok(Self::time_subfolders(
                MONTH_FIELD,
                &Self::months_in_year(quantization),
            ));
        }
        if time.day.is_none() {
            let year = Self::parse_year(time.year.unwrap(), path)?;
            let month = Self::parse_month(time.month.unwrap())?;
            return Ok(Self::time_subfolders(
                DAY_FIELD,
                &Self::days_in_month_for_quantization(quantization, year, month)?,
            ));
        }
        if time.hour.is_none() {
            let year = Self::parse_year(time.year.unwrap(), path)?;
            let month = Self::parse_month(time.month.unwrap())?;
            let days_in_month = Self::days_in_month(year, month)?;
            Self::parse_day(time.day.unwrap(), days_in_month, path)?;
            return Ok(Self::time_subfolders(
                HOUR_FIELD,
                &Self::hours_in_day(quantization),
            ));
        }

        let year = Self::parse_year(time.year.unwrap(), path)?;
        let month = Self::parse_month(time.month.unwrap())?;
        let days_in_month = Self::days_in_month(year, month)?;
        Self::parse_day(time.day.unwrap(), days_in_month, path)?;
        Self::parse_hour(time.hour.unwrap(), path)?;
        Ok(Vec::new())
    }

    fn time_subfolders(field: &str, values: &[i32]) -> Vec<String> {
        values
            .iter()
            .map(|value| format!("{field}{value:02}/"))
            .collect()
    }

    fn months_in_year(quantization: Quantization) -> Vec<i32> {
        match quantization {
            Quantization::Minutes(_)
            | Quantization::Hour(_)
            | Quantization::Day
            | Quantization::Week(_) => (1..=12).collect(),
            Quantization::Month(top_of_month) => (1..=12).step_by(top_of_month as usize).collect(),
            Quantization::Year => vec![1],
        }
    }

    fn days_in_month_for_quantization(
        quantization: Quantization,
        year: i32,
        month: u32,
    ) -> BpResult<Vec<i32>> {
        let days_in_month = Self::days_in_month(year, month)?;
        match quantization {
            Quantization::Minutes(_) | Quantization::Hour(_) | Quantization::Day => {
                Ok((1..=days_in_month as i32).collect())
            }
            Quantization::Week(weekday) => Ok((1..=days_in_month)
                .filter(|day| {
                    NaiveDate::from_ymd_opt(year, month, *day)
                        .is_some_and(|date| date.weekday() == weekday)
                })
                .map(|day| day as i32)
                .collect()),
            Quantization::Month(top_of_month) => {
                Ok(if (month - 1).is_multiple_of(top_of_month as u32) {
                    vec![1]
                } else {
                    Vec::new()
                })
            }
            Quantization::Year => Ok(if month == 1 { vec![1] } else { Vec::new() }),
        }
    }

    fn hours_in_day(quantization: Quantization) -> Vec<i32> {
        match quantization {
            Quantization::Minutes(_) => (0..24).collect(),
            Quantization::Hour(top_of_hour) => (0..24).step_by(top_of_hour as usize).collect(),
            Quantization::Day
            | Quantization::Week(_)
            | Quantization::Month(_)
            | Quantization::Year => vec![0],
        }
    }

    fn parse_year(segment: i32, path: &str) -> BpResult<i32> {
        if segment <= 0 {
            return Err(BpError::Data(DataError::InvalidYearMonth {
                path: path.to_string(),
            }));
        }
        Ok(segment)
    }

    fn parse_month(month: u32) -> BpResult<u32> {
        if (1..=12).contains(&month) {
            Ok(month)
        } else {
            Err(BpError::Data(DataError::InvalidMonth { month }))
        }
    }

    fn parse_day(day: u32, days_in_month: u32, _path: &str) -> BpResult<u32> {
        if (1..=days_in_month).contains(&day) {
            Ok(day)
        } else {
            Err(BpError::Data(DataError::InvalidDay { day }))
        }
    }

    fn parse_hour(hour: u32, _path: &str) -> BpResult<u32> {
        if hour < 24 {
            Ok(hour)
        } else {
            Err(BpError::Data(DataError::InvalidHour { hour }))
        }
    }

    fn days_in_month(year: i32, month: u32) -> BpResult<u32> {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => Ok(31),
            4 | 6 | 9 | 11 => Ok(30),
            2 => {
                if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                    Ok(29)
                } else {
                    Ok(28)
                }
            }
            _ => Err(BpError::Data(DataError::InvalidMonth { month })),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TimeFields {
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    hour: Option<u32>,
}

impl TimeFields {
    fn parse(path: &str) -> BpResult<Self> {
        let segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let Some(year_index) = segments
            .iter()
            .position(|segment| segment.starts_with(YEAR_FIELD))
        else {
            return Ok(Self::default());
        };

        let time_segments = &segments[year_index..];
        if time_segments.len() > TIME_FIELDS.len() {
            return Err(BpError::Data(DataError::TooManyPathSegments {
                path: path.to_string(),
            }));
        }

        let mut fields = Self::default();
        for (index, (segment, field)) in time_segments.iter().zip(TIME_FIELDS).enumerate() {
            let Some(value) = segment.strip_prefix(field) else {
                return Err(BpError::Data(DataError::InvalidYearMonth {
                    path: path.to_string(),
                }));
            };
            let value = value.parse::<i32>().map_err(|_| {
                BpError::Data(DataError::InvalidYearMonth {
                    path: path.to_string(),
                })
            })?;
            match index {
                0 => fields.year = Some(value),
                1 => fields.month = Some(value as u32),
                2 => fields.day = Some(value as u32),
                3 => fields.hour = Some(value as u32),
                _ => unreachable!("time field index is bounded by TIME_FIELDS"),
            }
        }
        Ok(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quantized_time::{TopOfHour, TopOfMinute, TopOfMonth};
    use chrono::Weekday;

    const PAIR: &str = "raw/events";

    fn subfolders(field: &str, values: &[i32]) -> Vec<String> {
        values
            .iter()
            .map(|value| format!("{field}{value:02}/"))
            .collect()
    }

    #[test]
    fn months_follow_quantization() {
        let path = format!("{PAIR}/year=2024/");
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &path,
                Quantization::Minutes(TopOfMinute::Six)
            )
            .unwrap(),
            subfolders(MONTH_FIELD, &(1..=12).collect::<Vec<_>>())
        );
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &path,
                Quantization::Month(TopOfMonth::Four)
            )
            .unwrap(),
            subfolders(MONTH_FIELD, &[1, 5, 9])
        );
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(&path, Quantization::Year).unwrap(),
            subfolders(MONTH_FIELD, &[1])
        );
    }

    #[test]
    fn days_follow_quantization() {
        let february = format!("{PAIR}/year=2024/month=02/");
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &february,
                Quantization::Minutes(TopOfMinute::Six)
            )
            .unwrap(),
            subfolders(DAY_FIELD, &(1..=29).collect::<Vec<_>>())
        );
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &february,
                Quantization::Week(Weekday::Mon)
            )
            .unwrap(),
            subfolders(DAY_FIELD, &[5, 12, 19, 26])
        );
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &february,
                Quantization::Month(TopOfMonth::Six)
            )
            .unwrap(),
            Vec::<String>::new()
        );

        let january = format!("{PAIR}/year=2024/month=01/");
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &january,
                Quantization::Month(TopOfMonth::Six)
            )
            .unwrap(),
            subfolders(DAY_FIELD, &[1])
        );
    }

    #[test]
    fn hours_follow_quantization() {
        let path = format!("{PAIR}/year=2024/month=01/day=01/");
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &path,
                Quantization::Minutes(TopOfMinute::Six)
            )
            .unwrap(),
            subfolders(HOUR_FIELD, &(0..24).collect::<Vec<_>>())
        );
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &path,
                Quantization::Hour(TopOfHour::Six)
            )
            .unwrap(),
            subfolders(HOUR_FIELD, &[0, 6, 12, 18])
        );
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(&path, Quantization::Day).unwrap(),
            subfolders(HOUR_FIELD, &[0])
        );
    }

    #[test]
    fn a_complete_hour_has_no_expected_child_folders() {
        let path = format!("{PAIR}/year=2024/month=01/day=01/hour=00/");
        assert_eq!(
            SubFolders::subfolders_from_path_with_quantization(
                &path,
                Quantization::Minutes(TopOfMinute::Six)
            )
            .unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn paths_without_archive_time_fields_are_rejected() {
        assert!(
            SubFolders::subfolders_from_path_with_quantization(
                "raw/events/",
                Quantization::Minutes(TopOfMinute::Six)
            )
            .is_err()
        );
        assert!(
            SubFolders::subfolders_from_path_with_quantization(
                "raw/events/2024/01/01/00/",
                Quantization::Minutes(TopOfMinute::Six)
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_archive_fields_are_rejected() {
        assert!(
            SubFolders::subfolders_from_path_with_quantization(
                format!("{PAIR}/year=2024/month=13/").as_str(),
                Quantization::Minutes(TopOfMinute::Six)
            )
            .is_err()
        );
        assert!(
            SubFolders::subfolders_from_path_with_quantization(
                format!("{PAIR}/year=2024/month=01/day=01/hour=24/").as_str(),
                Quantization::Minutes(TopOfMinute::Six)
            )
            .is_err()
        );
        assert!(
            SubFolders::subfolders_from_path_with_quantization(
                format!("{PAIR}/year=2024/month=01/day=01/hour=00/extra/").as_str(),
                Quantization::Minutes(TopOfMinute::Six)
            )
            .is_err()
        );
    }
}
