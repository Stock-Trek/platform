use platform_errors::{BpError, BpResult, ValidationError};

pub struct NatsSubject;

impl NatsSubject {
    /// Converts an arbitrary value into a safe single-token subject segment.
    pub fn normalize_segment(value: &str) -> String {
        value
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                    character.to_ascii_lowercase()
                } else {
                    '.'
                }
            })
            .collect()
    }

    pub fn validate(subject: &str) -> BpResult<()> {
        Self::validate_named("NATS subject", subject)
    }

    /// Validates a NATS subject, reporting errors against `name` so callers can
    /// identify the configuration value that was invalid.
    pub fn validate_named(name: &str, subject: &str) -> BpResult<()> {
        let invalid = subject.is_empty()
            || subject.split('.').any(|token| {
                token.is_empty()
                    || token.chars().any(|character| {
                        character.is_whitespace() || matches!(character, '>' | '*')
                    })
            });
        if invalid {
            return Err(BpError::Validation(ValidationError::InvalidValue {
                name: name.to_string(),
                value: subject.to_string(),
            }));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_publish_subjects_with_wildcards_or_empty_tokens() {
        assert!(NatsSubject::validate("market.events.binance.btcusdt.trade").is_ok());
        assert!(NatsSubject::validate("market..trade").is_err());
        assert!(NatsSubject::validate("market.*").is_err());
        assert!(NatsSubject::validate("market.>").is_err());
        assert!(NatsSubject::validate("market trade").is_err());
    }
}
