use crate::{TokenMetadata, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use serde_json::{Value, json};

const EXPIRES_AT: &str = "expires_at";
const SCOPES: &str = "scopes";
const REVOKED_AT: &str = "revoked_at";
const REVOCATION_REASON: &str = "revocation_reason";
const REVOKED_BY: &str = "revoked_by";
const LAST_USED_AT: &str = "last_used_at";
const ROTATED_FROM: &str = "rotated_from";
const REPLACED_BY: &str = "replaced_by";
const ROTATION_OVERLAP_UNTIL: &str = "rotation_overlap_until";

impl TokenMetadata {
    pub fn to_value(&self) -> Value {
        json!({
            EXPIRES_AT: self.expires_at.to_rfc3339(),
            SCOPES: self.scopes,
            REVOKED_AT: self.revoked_at.map(|value| value.to_rfc3339()),
            REVOCATION_REASON: self.revocation_reason,
            REVOKED_BY: self.revoked_by,
            LAST_USED_AT: self.last_used_at.map(|value| value.to_rfc3339()),
            ROTATED_FROM: self.rotated_from,
            REPLACED_BY: self.replaced_by,
            ROTATION_OVERLAP_UNTIL: self.rotation_overlap_until.map(|value| value.to_rfc3339()),
        })
    }

    pub fn from_value(value: &Value) -> BpResult<Self> {
        let scopes = match value.get(SCOPES) {
            Some(Value::Array(scopes)) => scopes
                .iter()
                .map(|scope| {
                    scope.as_str().map(str::to_string).ok_or_else(|| {
                        BpError::auth_invalid_field(SCOPES, "must contain only strings")
                    })
                })
                .collect::<BpResult<Vec<_>>>()?,
            Some(_) => {
                return Err(BpError::auth_invalid_field(
                    SCOPES,
                    "must be an array of strings",
                ));
            }
            None => return Err(BpError::auth_invalid_field(SCOPES, "is required")),
        };
        TokenScope::validate_all(&scopes)?;
        Ok(Self {
            expires_at: Self::timestamp(value, EXPIRES_AT)?
                .ok_or_else(|| BpError::auth_invalid_field(EXPIRES_AT, "is required"))?,
            scopes,
            revoked_at: Self::timestamp(value, REVOKED_AT)?,
            revocation_reason: Self::string(value, REVOCATION_REASON)?,
            revoked_by: Self::string(value, REVOKED_BY)?,
            last_used_at: Self::timestamp(value, LAST_USED_AT)?,
            rotated_from: Self::string(value, ROTATED_FROM)?,
            replaced_by: Self::string(value, REPLACED_BY)?,
            rotation_overlap_until: Self::timestamp(value, ROTATION_OVERLAP_UNTIL)?,
        })
    }

    fn string(value: &Value, field: &'static str) -> BpResult<Option<String>> {
        match value.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(BpError::auth_invalid_field(
                field,
                "must be a string or null",
            )),
        }
    }

    fn timestamp(value: &Value, field: &'static str) -> BpResult<Option<DateTime<Utc>>> {
        let Some(value) = Self::string(value, field)? else {
            return Ok(None);
        };
        DateTime::parse_from_rfc3339(&value)
            .map(|value| Some(value.with_timezone(&Utc)))
            .map_err(|e| {
                BpError::auth_invalid_field(field, format!("not an RFC 3339 timestamp: {e}"))
            })
    }
}
