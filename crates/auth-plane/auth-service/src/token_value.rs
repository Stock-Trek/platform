use crate::{ApiToken, TokenMetadata};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Map, Value};

const CREATED_AT: &str = "created_at";
const ID: &str = "id";
const LAST_USED_AT: &str = "last_used_at";
const OWNER: &str = "owner";
const SECRET: &str = "secret";

impl ApiToken {
    pub fn to_value(&self) -> Value {
        let mut value = Map::new();
        value.insert(ID.to_string(), Value::String(self.id.clone()));
        value.insert(
            CREATED_AT.to_string(),
            Value::String(self.created_at.to_rfc3339()),
        );
        value.insert(OWNER.to_string(), Value::String(self.owner.clone()));
        value.insert(
            SECRET.to_string(),
            Value::String(self.secret.expose_secret().to_string()),
        );
        if let Value::Object(metadata) = self.metadata.to_value() {
            value.extend(metadata);
        }
        Value::Object(value)
    }

    pub fn same_rotation_replacement(&self, other: &Self) -> bool {
        let mut left = self.to_value();
        let mut right = other.to_value();
        if let (Some(left), Some(right)) = (left.as_object_mut(), right.as_object_mut()) {
            left.remove(LAST_USED_AT);
            right.remove(LAST_USED_AT);
        }
        left == right
    }

    pub fn from_value(id: &str, value: &Value) -> PlatformResult<Self> {
        let stored_id = Self::string_field(value, ID)?;
        if stored_id != id {
            return Err(PlatformError::auth_invalid_field(
                ID,
                "does not match the requested token id",
            ));
        }
        let owner = Self::string_field(value, OWNER)?;
        let secret = Self::string_field(value, SECRET)?;
        let created_at = Self::string_field(value, CREATED_AT)?;
        let created_at = DateTime::parse_from_rfc3339(&created_at)
            .map_err(|e| {
                PlatformError::auth_invalid_field(
                    CREATED_AT,
                    format!("not an RFC 3339 timestamp: {e}"),
                )
            })?
            .with_timezone(&Utc);
        let metadata = TokenMetadata::from_value(value)?;
        Self::from_parts(id, owner, SecretString::from(secret), created_at, metadata)
    }

    fn string_field(value: &Value, field: &'static str) -> PlatformResult<String> {
        value
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                PlatformError::auth_invalid_field(field, "missing from the stored token value")
            })
    }
}
