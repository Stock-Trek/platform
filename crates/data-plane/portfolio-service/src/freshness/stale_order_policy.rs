use platform_errors::PlatformError;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleOrderPolicy {
    Allow,
    Degrade,
    Reject,
}

impl FromStr for StaleOrderPolicy {
    type Err = PlatformError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "allow" => Ok(Self::Allow),
            "degrade" => Ok(Self::Degrade),
            "reject" => Ok(Self::Reject),
            _ => Err(PlatformError::bad_value(
                "stale order policy",
                value,
                "must be allow, degrade, or reject",
            )),
        }
    }
}
