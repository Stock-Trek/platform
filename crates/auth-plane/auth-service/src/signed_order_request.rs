use crate::OrderRequestContext;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

const ENVELOPE_VERSION: &str = "v1";
pub(crate) const MAX_ENVELOPE_LENGTH: usize = 1024;

pub struct SignedOrderRequest {
    token_id: String,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    nonce: String,
    envelope: String,
}

impl SignedOrderRequest {
    pub fn from_context(
        context: &OrderRequestContext,
        signature: impl Into<String>,
    ) -> BpResult<Self> {
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
    ) -> BpResult<Self> {
        let token_id = token_id.into();
        let nonce = nonce.into();
        let signature = signature.into();
        let issued_at = OrderRequestContext::whole_seconds(issued_at);
        let expires_at = OrderRequestContext::whole_seconds(expires_at);
        let envelope = Self::encode(&token_id, issued_at, expires_at, &nonce, &signature);
        Self::decode(&envelope)
    }

    pub fn decode(signature: &str) -> BpResult<Self> {
        if signature.len() > MAX_ENVELOPE_LENGTH {
            return Err(BpError::auth_invalid_field(
                "signature",
                format!("must be at most {MAX_ENVELOPE_LENGTH} characters"),
            ));
        }
        let mut parts = signature.split('.');
        let version = parts.next();
        let issued_at = parts.next();
        let expires_at = parts.next();
        let token_id = parts.next();
        let nonce = parts.next();
        let raw_signature = parts.next();
        let (
            Some(version),
            Some(issued_at),
            Some(expires_at),
            Some(token_id),
            Some(nonce),
            Some(raw_signature),
        ) = (
            version,
            issued_at,
            expires_at,
            token_id,
            nonce,
            raw_signature,
        )
        else {
            return Err(BpError::auth_invalid_field(
                "signature",
                "is not a supported signed order request envelope",
            ));
        };
        if version != ENVELOPE_VERSION || parts.next().is_some() || raw_signature.is_empty() {
            return Err(BpError::auth_invalid_field(
                "signature",
                "is not a supported signed order request envelope",
            ));
        }
        let issued_at = Self::parse_timestamp(issued_at)?;
        let expires_at = Self::parse_timestamp(expires_at)?;
        OrderRequestContext::new(token_id, issued_at, expires_at, nonce)?;
        let signed = Self {
            token_id: token_id.to_string(),
            issued_at,
            expires_at,
            nonce: nonce.to_string(),
            envelope: Self::encode(token_id, issued_at, expires_at, nonce, raw_signature),
        };
        if signed.envelope != signature {
            return Err(BpError::auth_invalid_field(
                "signature",
                "must use canonical timestamp encodings",
            ));
        }
        Ok(signed)
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

    pub fn envelope(&self) -> &str {
        &self.envelope
    }

    pub fn signature(&self) -> &str {
        self.envelope
            .rsplit_once('.')
            .map_or(self.envelope.as_str(), |(_, signature)| signature)
    }

    pub fn raw_signature(&self) -> &str {
        self.signature()
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
            .field("envelope", &"<redacted>")
            .finish()
    }
}
