use platform_errors::BuildError;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleOrderPolicy {
    Allow,
    Degrade,
    Reject,
}

impl FromStr for StaleOrderPolicy {
    type Err = BuildError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "allow" => Ok(Self::Allow),
            "degrade" => Ok(Self::Degrade),
            "reject" => Ok(Self::Reject),
            _ => Err(BuildError::bad_value(
                "stale order policy",
                value,
                "must be allow, degrade, or reject",
            )),
        }
    }
}
