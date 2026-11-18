use crate::api::Problem;

pub struct CronExpression;

const FIELDS: usize = 5;
const FIELD_BOUNDS: [(u32, u32); FIELDS] = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)];

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
