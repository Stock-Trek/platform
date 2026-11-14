use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TokenAuditAction {
    Issued,
    Deleted,
    Authenticated,
    AuthenticationFailed,
    Signed,
    SignatureVerified,
    SignatureRejected,
    Rotated,
    Revoked,
}
