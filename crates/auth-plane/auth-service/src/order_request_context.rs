use crate::ApiToken;
use chrono::{DateTime, Duration, Utc};
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};

const NONCE_LENGTH: usize = 32;
const PROTOCOL: &str = "platform-auth-service/order-request";
const VERSION: &str = "v1";
const ALGORITHM: &str = "HMAC-SHA256";

pub struct OrderRequestContext {
    token_id: String,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    nonce: String,
}

impl OrderRequestContext {
    pub fn new(
        token_id: impl Into<String>,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        nonce: impl Into<String>,
    ) -> BpResult<Self> {
        let token_id = token_id.into();
        ApiToken::validate_id(&token_id)?;
        let issued_at = Self::whole_seconds(issued_at);
        let expires_at = Self::whole_seconds(expires_at);
        let nonce = nonce.into();
        if nonce.is_empty()
            || !nonce
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(BpError::auth_invalid_field(
                "nonce",
                "must only contain ASCII letters, digits, '-' and '_'",
            ));
        }
        if expires_at <= issued_at {
            return Err(BpError::auth_invalid_field(
                "expires_at",
                "must be after the issue time",
            ));
        }
        Ok(Self {
            token_id,
            issued_at,
            expires_at,
            nonce,
        })
    }

    pub fn generate(
        token_id: impl Into<String>,
        issued_at: DateTime<Utc>,
        validity: Duration,
    ) -> BpResult<Self> {
        let nonce: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(NONCE_LENGTH)
            .map(char::from)
            .collect();
        let issued_at = Self::whole_seconds(issued_at);
        let expires_at = issued_at.checked_add_signed(validity).ok_or_else(|| {
            BpError::auth_invalid_field("expires_at", "is outside the supported range")
        })?;
        Self::new(token_id, issued_at, expires_at, nonce)
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn issued_at(&self) -> DateTime<Utc> {
        self.issued_at
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    pub fn version(&self) -> &'static str {
        VERSION
    }

    pub fn algorithm(&self) -> &'static str {
        ALGORITHM
    }

    pub fn canonical(&self, request: &[u8]) -> Vec<u8> {
        let mut canonical = Vec::new();
        Self::append_field(&mut canonical, "protocol", PROTOCOL.as_bytes());
        Self::append_field(&mut canonical, "version", VERSION.as_bytes());
        Self::append_field(&mut canonical, "algorithm", ALGORITHM.as_bytes());
        Self::append_field(&mut canonical, "token_id", self.token_id.as_bytes());
        Self::append_field(
            &mut canonical,
            "issued_at",
            self.issued_at.timestamp().to_string().as_bytes(),
        );
        Self::append_field(
            &mut canonical,
            "expires_at",
            self.expires_at.timestamp().to_string().as_bytes(),
        );
        Self::append_field(&mut canonical, "nonce", self.nonce.as_bytes());
        Self::append_field(&mut canonical, "request", request);
        canonical
    }

    fn append_field(output: &mut Vec<u8>, name: &str, value: &[u8]) {
        output.extend_from_slice(name.as_bytes());
        output.push(b':');
        output.extend_from_slice(&(value.len() as u64).to_be_bytes());
        output.extend_from_slice(value);
    }

    pub(crate) fn whole_seconds(value: DateTime<Utc>) -> DateTime<Utc> {
        DateTime::from_timestamp(value.timestamp(), 0).unwrap_or(value)
    }
}
