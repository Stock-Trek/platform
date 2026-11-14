use platform_errors::{PlatformError, PlatformResult, ValidationError};

pub struct NatsSubject;

impl NatsSubject {
    /// Converts an arbitrary value into a safe single-token subject segment.
    ///
    /// Runs of characters that are not legal in a subject token collapse into a
    /// single `-` so normalization can never split the value across tokens. A
    /// value with no usable characters becomes `unknown`.
    pub fn normalize_segment(value: &str) -> String {
        let mut normalized = String::with_capacity(value.len());
        let mut separator_pending = false;
        for character in value.chars() {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                if separator_pending && !normalized.is_empty() && !matches!(character, '-' | '_') {
                    normalized.push('-');
                }
                separator_pending = false;
                normalized.push(character.to_ascii_lowercase());
            } else {
                separator_pending = true;
            }
        }
        if normalized.is_empty() {
            normalized.push_str("unknown");
        }
        normalized
    }

    pub fn validate(subject: &str) -> PlatformResult<()> {
        Self::validate_named("NATS subject", subject)
    }

    /// Validates a NATS subject, reporting errors against `name` so callers can
    /// identify the configuration value that was invalid.
    pub fn validate_named(name: &str, subject: &str) -> PlatformResult<()> {
        let invalid = subject.is_empty()
            || subject.split('.').any(|token| {
                token.is_empty()
                    || token.chars().any(|character| {
                        character.is_whitespace() || matches!(character, '>' | '*')
                    })
            });
        if invalid {
            return Err(PlatformError::Validation(ValidationError::InvalidValue {
                name: name.to_string(),
                value: subject.to_string(),
            }));
        }
        Ok(())
    }
}
