use platform_errors::{BpError, BpResult, ValidationError};

pub struct NatsSubject;

impl NatsSubject {
    /// Converts an arbitrary value into a safe subject segment.
    ///
    /// Illegal characters and runs of dots collapse into a single separator so
    /// normalization can never create empty subject tokens. A value with no
    /// usable characters becomes `unknown`.
    pub fn normalize_segment(value: &str) -> String {
        let mut normalized = String::with_capacity(value.len());
        for character in value.chars() {
            let character = if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '.'
            };
            if character == '.' {
                if !normalized.is_empty() && !normalized.ends_with('.') {
                    normalized.push(character);
                }
            } else {
                normalized.push(character);
            }
        }
        if normalized.ends_with('.') {
            normalized.pop();
        }
        if normalized.is_empty() {
            normalized.push_str("unknown");
        }
        normalized
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
mod tests;
