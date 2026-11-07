use crate::{ApiToken, TokenMetadata};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Map, Value, json};

const CREATED_AT: &str = "created_at";
const OWNER: &str = "owner";
const SECRET: &str = "secret";

impl ApiToken {
    pub fn to_value(&self) -> Value {
        let mut value = match json!({
            CREATED_AT: self.created_at.to_rfc3339(),
            OWNER: self.owner,
            SECRET: self.secret.expose_secret(),
        }) {
            Value::Object(value) => value,
            _ => Map::new(),
        };
        if let Value::Object(metadata) = self.metadata.to_value() {
            value.extend(metadata);
        }
        Value::Object(value)
    }

    pub fn from_value(id: &str, value: &Value) -> BpResult<Self> {
        let owner = Self::string_field(value, OWNER)?;
        let secret = Self::string_field(value, SECRET)?;
        let created_at = Self::string_field(value, CREATED_AT)?;
        let created_at = DateTime::parse_from_rfc3339(&created_at)
            .map_err(|e| {
                BpError::auth_invalid_field(CREATED_AT, format!("not an RFC 3339 timestamp: {e}"))
            })?
            .with_timezone(&Utc);
        let metadata = TokenMetadata::from_value(value)?;
        Self::from_parts(id, owner, SecretString::from(secret), created_at, metadata)
    }

    fn string_field(value: &Value, field: &'static str) -> BpResult<String> {
        value
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                BpError::auth_invalid_field(field, "missing from the stored token value")
            })
    }
}
