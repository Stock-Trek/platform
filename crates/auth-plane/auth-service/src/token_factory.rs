use crate::{ApiToken, TokenMetadata};
use chrono::{DateTime, Utc};
use platform_errors::BuildResult;
use rand::{Rng, distributions::Alphanumeric};
use secrecy::SecretString;

const SECRET_LENGTH: usize = 32;

impl ApiToken {
    pub fn generate(
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        metadata: TokenMetadata,
    ) -> BuildResult<Self> {
        let secret: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(SECRET_LENGTH)
            .map(char::from)
            .collect();
        Self::from_parts(id, owner, SecretString::from(secret), created_at, metadata)
    }
}
