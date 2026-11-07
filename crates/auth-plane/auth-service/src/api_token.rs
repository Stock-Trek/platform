use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use subtle::ConstantTimeEq;

const SECRET_LENGTH: usize = 32;
const CREATED_AT: &str = "created_at";
const OWNER: &str = "owner";
const SECRET: &str = "secret";

#[derive(Clone)]
pub struct ApiToken {
    id: String,
    owner: String,
    secret: SecretString,
    created_at: DateTime<Utc>,
}

impl ApiToken {
    pub fn new(
        id: impl Into<String>,
        owner: impl Into<String>,
        secret: SecretString,
        created_at: DateTime<Utc>,
    ) -> BpResult<Self> {
        let id = id.into();
        Self::validate_id(&id)?;
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        if secret.expose_secret().is_empty() {
            return Err(BpError::auth_invalid_field("secret", "must not be empty"));
        }
        Ok(Self {
            id,
            owner,
            secret,
            created_at,
        })
    }

    pub fn generate(id: impl Into<String>, owner: impl Into<String>) -> BpResult<Self> {
        let secret: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(SECRET_LENGTH)
            .map(char::from)
            .collect();
        Self::new(id, owner, SecretString::from(secret), Utc::now())
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn secret(&self) -> &SecretString {
        &self.secret
    }

    pub fn matches_secret(&self, candidate: &str) -> bool {
        self.secret
            .expose_secret()
            .as_bytes()
            .ct_eq(candidate.as_bytes())
            .into()
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn to_value(&self) -> Value {
        serde_json::json!({
            CREATED_AT: self.created_at.to_rfc3339(),
            OWNER: self.owner,
            SECRET: self.secret.expose_secret(),
        })
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
        Self::new(id, owner, SecretString::from(secret), created_at)
    }

    pub fn validate_id(id: &str) -> BpResult<()> {
        let valid = !id.is_empty()
            && id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if valid {
            Ok(())
        } else {
            Err(BpError::auth_invalid_field(
                "token_id",
                format!("{id:?} must only contain ASCII letters, digits, '-' and '_'"),
            ))
        }
    }

    pub fn validate_token_id(id: &str) -> BpResult<()> {
        Self::validate_id(id)
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

impl std::fmt::Debug for ApiToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiToken")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("secret", &"<redacted>")
            .field("created_at", &self.created_at)
            .finish()
    }
}
