use platform_errors::{BpError, BpResult};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RevocationReason {
    Compromised,
    Expired,
    NoLongerNeeded,
    OwnerRequested,
    Rotated,
    Administrative,
}

impl RevocationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compromised => "compromised",
            Self::Expired => "expired",
            Self::NoLongerNeeded => "no_longer_needed",
            Self::OwnerRequested => "owner_requested",
            Self::Rotated => "rotated",
            Self::Administrative => "administrative",
        }
    }
}

impl FromStr for RevocationReason {
    type Err = BpError;

    fn from_str(value: &str) -> BpResult<Self> {
        match value {
            "compromised" => Ok(Self::Compromised),
            "expired" => Ok(Self::Expired),
            "no_longer_needed" | "no longer needed" => Ok(Self::NoLongerNeeded),
            "owner_requested" => Ok(Self::OwnerRequested),
            "rotated" => Ok(Self::Rotated),
            "administrative" => Ok(Self::Administrative),
            _ => Err(BpError::auth_invalid_field(
                "revocation_reason",
                format!("{value:?} is not a valid revocation reason"),
            )),
        }
    }
}
