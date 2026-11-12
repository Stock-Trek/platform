use crate::{TokenMetadata, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};
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
            SCOPES: self.scopes.iter().map(ToString::to_string).collect::<Vec<_>>(),
            REVOKED_AT: self.revoked_at.map(|value| value.to_rfc3339()),
            REVOCATION_REASON: self.revocation_reason.map(|reason| reason.to_string()),
            REVOKED_BY: self.revoked_by.map(|actor| actor.to_string()),
            LAST_USED_AT: self.last_used_at.map(|value| value.to_rfc3339()),
            ROTATED_FROM: self.rotated_from,
            REPLACED_BY: self.replaced_by,
            ROTATION_OVERLAP_UNTIL: self.rotation_overlap_until.map(|value| value.to_rfc3339()),
        })
    }

    pub fn from_value(value: &Value) -> BuildResult<Self> {
        let scopes = match value.get(SCOPES) {
            Some(Value::Array(scopes)) => scopes
                .iter()
                .map(|scope| {
                    let scope = scope.as_str().ok_or_else(|| {
                        BuildError::auth_invalid_field(SCOPES, "must contain only strings")
                    })?;
                    scope.parse::<TokenScope>().map_err(|_| {
                        BuildError::auth_invalid_field(
                            SCOPES,
                            format!("{scope:?} is not a valid token scope"),
                        )
                    })
                })
                .collect::<BuildResult<Vec<_>>>()?,
            Some(_) => {
                return Err(BuildError::auth_invalid_field(
                    SCOPES,
                    "must be an array of strings",
                ));
            }
            None => return Err(BuildError::auth_invalid_field(SCOPES, "is required")),
        };
        let expires_at = Self::timestamp(value, EXPIRES_AT)?
            .ok_or_else(|| BuildError::auth_invalid_field(EXPIRES_AT, "is required"))?;
        let mut metadata = Self::new(expires_at, scopes.iter())?;
        metadata.revoked_at = Self::timestamp(value, REVOKED_AT)?;
        metadata.revocation_reason = Self::enumeration(value, REVOCATION_REASON)?;
        metadata.revoked_by = Self::enumeration(value, REVOKED_BY)?;
        metadata.last_used_at = Self::timestamp(value, LAST_USED_AT)?;
        metadata.rotated_from = Self::string(value, ROTATED_FROM)?;
        metadata.replaced_by = Self::string(value, REPLACED_BY)?;
        metadata.rotation_overlap_until = Self::timestamp(value, ROTATION_OVERLAP_UNTIL)?;
        metadata.validate()?;
        Ok(metadata)
    }

    fn enumeration<T>(value: &Value, field: &'static str) -> BuildResult<Option<T>>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        let Some(value) = Self::string(value, field)? else {
            return Ok(None);
        };
        value
            .parse::<T>()
            .map(Some)
            .map_err(|error| BuildError::auth_invalid_field(field, error.to_string()))
    }

    fn string(value: &Value, field: &'static str) -> BuildResult<Option<String>> {
        match value.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(BuildError::auth_invalid_field(
                field,
                "must be a string or null",
            )),
        }
    }

    fn timestamp(value: &Value, field: &'static str) -> BuildResult<Option<DateTime<Utc>>> {
        let Some(value) = Self::string(value, field)? else {
            return Ok(None);
        };
        DateTime::parse_from_rfc3339(&value)
            .map(|value| Some(value.with_timezone(&Utc)))
            .map_err(|e| {
                BuildError::auth_invalid_field(field, format!("not an RFC 3339 timestamp: {e}"))
            })
    }
}
