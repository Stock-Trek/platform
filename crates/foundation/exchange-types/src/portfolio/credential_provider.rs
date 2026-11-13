use crate::portfolio::{AuthError, Credential};
use async_trait::async_trait;

#[async_trait]
pub trait CredentialProvider: Send + Sync {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError>;

    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError>;

    async fn revoke(&self, credential: &Credential) -> Result<(), AuthError>;
}
