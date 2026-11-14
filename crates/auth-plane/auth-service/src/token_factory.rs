use crate::{ApiToken, TokenMetadata, random_string::RandomString};
use chrono::{DateTime, Utc};
use platform_errors::PlatformResult;
use secrecy::SecretString;

const SECRET_LENGTH: usize = 32;

impl ApiToken {
    pub fn generate(
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        metadata: TokenMetadata,
    ) -> PlatformResult<Self> {
        let secret = RandomString::generate(SECRET_LENGTH);
        Self::from_parts(id, owner, SecretString::from(secret), created_at, metadata)
    }
}
