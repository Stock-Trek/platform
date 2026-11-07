use crate::{ApiToken, TokenMetadata, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use rand::{Rng, distributions::Alphanumeric};
use secrecy::SecretString;

const SECRET_LENGTH: usize = 32;

impl ApiToken {
    pub fn generate(id: impl Into<String>, owner: impl Into<String>) -> BpResult<Self> {
        Self::generate_at(id, owner, Utc::now())
    }

    pub fn generate_at(
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> BpResult<Self> {
        let metadata = TokenMetadata::new(None, TokenScope::default_scopes())?;
        Self::generate_with_metadata(id, owner, created_at, metadata)
    }

    pub fn generate_with_metadata(
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        metadata: TokenMetadata,
    ) -> BpResult<Self> {
        Self::from_parts(id, owner, Self::new_secret(), created_at, metadata)
    }

    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.metadata.expires_at = Some(expires_at);
        self
    }

    pub fn with_expires_at(self, expires_at: DateTime<Utc>) -> Self {
        self.with_expiry(expires_at)
    }

    pub fn without_expiry(mut self) -> Self {
        self.metadata.expires_at = None;
        self
    }

    pub fn with_scopes<S, I>(mut self, scopes: I) -> BpResult<Self>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let scopes: Vec<String> = scopes
            .into_iter()
            .map(|scope| scope.as_ref().to_string())
            .collect();
        TokenScope::validate_all(&scopes)?;
        self.metadata.scopes = scopes;
        Ok(self)
    }

    pub fn with_last_used_at(mut self, used_at: DateTime<Utc>) -> Self {
        self.metadata.last_used_at = Some(used_at);
        self
    }

    fn new_secret() -> SecretString {
        let secret: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(SECRET_LENGTH)
            .map(char::from)
            .collect();
        SecretString::from(secret)
    }
}
