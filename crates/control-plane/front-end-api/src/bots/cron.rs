use crate::api::Problem;
use chrono::{Datelike, NaiveDate};

pub struct CronExpression;

const FIELDS: usize = 5;
const FIELD_BOUNDS: [(u32, u32); FIELDS] = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)];
const REFERENCE_START: NaiveDate = NaiveDate::from_ymd_opt(2028, 1, 1).expect("reference date");
const REFERENCE_DAYS: u32 = 366;

impl CronExpression {
    pub fn validated(expression: &str) -> Result<(), Problem> {
        let fields: Vec<&str> = expression.split_whitespace().collect();
        if fields.len() != FIELDS {
            return Err(Problem::validation_failed(
                "cron expression must have five fields",
            ));
        }
        for (field, &(minimum, maximum)) in fields.iter().zip(FIELD_BOUNDS.iter()) {
            Self::validate_field(field, minimum, maximum)?;
        }
        Ok(())
    }

    pub fn runs_per_day(expression: &str) -> u64 {
        Self::matching_minutes(expression)
            .unwrap_or(0)
            .div_ceil(u64::from(REFERENCE_DAYS))
    }

    fn matching_minutes(expression: &str) -> Option<u64> {
        let fields: Vec<&str> = expression.split_whitespace().collect();
        if fields.len() != FIELDS {
            return None;
        }
        let minutes = Self::field_values(fields[0], 0, 59)?;
        let hours = Self::field_values(fields[1], 0, 23)?;
        let days = Self::field_values(fields[2], 1, 31)?;
        let months = Self::field_values(fields[3], 1, 12)?;
        let weekdays = Self::field_values(fields[4], 0, 7)?;
        let day_restricted = fields[2] != "*";
        let weekday_restricted = fields[4] != "*";
        let mut day = REFERENCE_START;
        let mut count = 0u64;
        for _ in 0..REFERENCE_DAYS {
            let day_matches = match (day_restricted, weekday_restricted) {
                (true, true) => {
                    days[day.day() as usize]
                        || weekdays[day.weekday().num_days_from_sunday() as usize]
                }
                (true, false) => days[day.day() as usize],
                (false, true) => weekdays[day.weekday().num_days_from_sunday() as usize],
                (false, false) => true,
            };
            if day_matches && months[day.month() as usize] {
                let active_hours = hours.iter().filter(|held| **held).count();
                let active_minutes = minutes.iter().filter(|held| **held).count();
                count += (active_hours * active_minutes) as u64;
            }
            day = day.succ_opt()?;
        }
        Some(count)
    }

    fn field_values(field: &str, minimum: u32, maximum: u32) -> Option<Vec<bool>> {
        let mut values = vec![false; (maximum + 1) as usize];
        for term in field.split(',') {
            let (base, step) = match term.split_once('/') {
                Some((base, step)) => (base, step.parse::<u32>().ok()?),
                None => (term, 1),
            };
            let (low, high) = match base.split_once('-') {
                Some((low, high)) => (low.parse::<u32>().ok()?, high.parse::<u32>().ok()?),
                None if base == "*" => (minimum, maximum),
                None => {
                    let value = base.parse::<u32>().ok()?;
                    if step > 1 {
                        (value, maximum)
                    } else {
                        (value, value)
                    }
                }
            };
            if step == 0 || low > high || high > maximum {
                return None;
            }
            let mut value = u64::from(low);
            let high = u64::from(high);
            let step = u64::from(step);
            while value <= high {
                values[value as usize] = true;
                value += step;
            }
        }
        Some(values)
    }

    fn validate_field(field: &str, minimum: u32, maximum: u32) -> Result<(), Problem> {
        for term in field.split(',') {
            Self::validate_term(term, minimum, maximum)?;
        }
        Ok(())
    }

    fn validate_term(term: &str, minimum: u32, maximum: u32) -> Result<(), Problem> {
        let (base, step) = match term.split_once('/') {
            Some((base, step)) => (base, Some(step)),
            None => (term, None),
        };
        Self::validate_base(base, minimum, maximum)?;
        if let Some(step) = step {
            Self::value(step, 1, u32::MAX)?;
        }
        Ok(())
    }

    fn validate_base(base: &str, minimum: u32, maximum: u32) -> Result<(), Problem> {
        if base == "*" {
            return Ok(());
        }
        let (low, high) = match base.split_once('-') {
            Some((low, high)) => (low, Some(high)),
            None => (base, None),
        };
        let low = Self::value(low, minimum, maximum)?;
        let high = match high {
            Some(high) => Self::value(high, minimum, maximum)?,
            None => low,
        };
        if low > high {
            return Err(Problem::validation_failed(
                "cron range start must not exceed its end",
            ));
        }
        Ok(())
    }

    fn value(value: &str, minimum: u32, maximum: u32) -> Result<u32, Problem> {
        let parsed: u32 = value
            .parse()
            .map_err(|_| Problem::validation_failed("cron values must be numbers"))?;
        if (minimum..=maximum).contains(&parsed) {
            Ok(parsed)
        } else {
            Err(Problem::validation_failed(format!(
                "cron value must be between {minimum} and {maximum}"
            )))
        }
    }
}

pub struct TimezoneName;

impl TimezoneName {
    pub fn validated(name: &str) -> Result<(), Problem> {
        let valid = !name.is_empty()
            && name.len() <= 64
            && name
                .chars()
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && name.split('/').all(|segment| {
                !segment.is_empty()
                    && segment.chars().all(|character| {
                        character.is_ascii_alphanumeric()
                            || matches!(character, '_' | '-' | '+' | '.')
                    })
            });
        if valid {
            Ok(())
        } else {
            Err(Problem::validation_failed(
                "timezone must be an IANA time-zone name",
            ))
        }
    }
}
