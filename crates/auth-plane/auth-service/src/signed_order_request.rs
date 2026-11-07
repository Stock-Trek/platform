use crate::OrderRequestContext;
use chrono::{DateTime, Duration, Utc};
use platform_errors::{BpError, BpResult};

const ENVELOPE_VERSION: &str = "v1";
const DEFAULT_VALIDITY_SECONDS: i64 = 300;

pub struct SignedOrderRequest {
    token_id: String,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    nonce: String,
    signature: String,
}

impl SignedOrderRequest {
    pub fn new(token_id: impl Into<String>, signature: impl Into<String>) -> Self {
        let token_id = token_id.into();
        let signature = signature.into();
        if let Ok(decoded) = Self::decode(&signature) {
            return decoded;
        }
        let issued_at = Utc::now();
        let expires_at = issued_at + Duration::seconds(DEFAULT_VALIDITY_SECONDS);
        let nonce = OrderRequestContext::generate(
            token_id.clone(),
            issued_at,
            Duration::seconds(DEFAULT_VALIDITY_SECONDS),
        )
        .map(|context| context.nonce().to_string())
        .unwrap_or_else(|_| "invalid".to_string());
        Self::from_parts(token_id, issued_at, expires_at, nonce, signature)
    }

    pub fn from_context(context: &OrderRequestContext, signature: impl Into<String>) -> Self {
        Self::from_parts(
            context.token_id(),
            context.issued_at(),
            context.expires_at(),
            context.nonce(),
            signature,
        )
    }

    pub fn from_parts(
        token_id: impl Into<String>,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        nonce: impl Into<String>,
        signature: impl Into<String>,
    ) -> Self {
        let token_id = token_id.into();
        let nonce = nonce.into();
        let signature = Self::encode(&token_id, issued_at, expires_at, &nonce, &signature.into());
        Self {
            token_id,
            issued_at,
            expires_at,
            nonce,
            signature,
        }
    }

    pub fn decode(signature: &str) -> BpResult<Self> {
        let mut parts = signature.split('.');
        let version = parts.next();
        let issued_at = parts.next();
        let expires_at = parts.next();
        let token_id = parts.next();
        let nonce = parts.next();
        let raw_signature = parts.next();
        if version != Some(ENVELOPE_VERSION)
            || parts.next().is_some()
            || issued_at.is_none()
            || expires_at.is_none()
            || nonce.is_none()
            || token_id.is_none()
            || raw_signature.is_none()
        {
            return Err(BpError::auth_invalid_field(
                "signature",
                "is not a supported signed order request envelope",
            ));
        }
        let token_id = token_id.unwrap_or_default();
        let nonce = nonce.unwrap_or_default();
        let issued_at = Self::parse_timestamp(issued_at.unwrap_or_default())?;
        let expires_at = Self::parse_timestamp(expires_at.unwrap_or_default())?;
        OrderRequestContext::new(token_id, issued_at, expires_at, nonce)?;
        Ok(Self {
            token_id: token_id.to_string(),
            issued_at,
            expires_at,
            nonce: nonce.to_string(),
            signature: signature.to_string(),
        })
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

    pub fn signature(&self) -> &str {
        &self.signature
    }

    pub fn raw_signature(&self) -> &str {
        self.signature
            .rsplit_once('.')
            .map(|(_, signature)| signature)
            .unwrap_or(self.signature.as_str())
    }

    pub fn context(&self) -> BpResult<OrderRequestContext> {
        OrderRequestContext::new(
            self.token_id.clone(),
            self.issued_at,
            self.expires_at,
            self.nonce.clone(),
        )
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now
    }

    fn encode(
        token_id: &str,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        nonce: &str,
        signature: &str,
    ) -> String {
        format!(
            "{ENVELOPE_VERSION}.{}.{}.{token_id}.{nonce}.{signature}",
            issued_at.timestamp(),
            expires_at.timestamp()
        )
    }

    fn parse_timestamp(value: &str) -> BpResult<DateTime<Utc>> {
        value
            .parse::<i64>()
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .ok_or_else(|| BpError::auth_invalid_field("signature", "has an invalid timestamp"))
    }
}

impl std::fmt::Debug for SignedOrderRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedOrderRequest")
            .field("token_id", &self.token_id)
            .field("issued_at", &self.issued_at)
            .field("expires_at", &self.expires_at)
            .field("nonce", &self.nonce)
            .field("signature", &"<redacted>")
            .finish()
    }
}
