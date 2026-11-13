use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::portfolio::credential_client::BinanceListenKeyClient,
    portfolio::{AuthError, Credential, CredentialProvider},
};
use async_trait::async_trait;
use std::time::{Duration, Instant};

pub struct BinanceCredentialProvider<C> {
    client: C,
    credentials: ApiKeyCredentials,
    ttl: Duration,
}

impl<C> BinanceCredentialProvider<C> {
    pub fn new(client: C, credentials: ApiKeyCredentials) -> Self {
        Self {
            client,
            credentials,
            ttl: Duration::from_mins(60),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    fn credential(&self, value: String) -> Credential {
        let issued_at = Instant::now();
        Credential {
            value,
            issued_at,
            expires_at: Some(issued_at + self.ttl),
            renewable: true,
        }
    }
}

#[async_trait]
impl<C> CredentialProvider for BinanceCredentialProvider<C>
where
    C: BinanceListenKeyClient,
{
    async fn acquire(&self) -> Result<Option<Credential>, AuthError> {
        let listen_key = self
            .client
            .create_listen_key(&self.credentials)
            .await
            .map_err(|error| AuthError::Acquire(error.to_string()))?;
        Ok(Some(self.credential(listen_key)))
    }

    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError> {
        self.client
            .keepalive_listen_key(&self.credentials, &credential.value)
            .await
            .map_err(|error| AuthError::Renew(error.to_string()))?;
        Ok(self.credential(credential.value.clone()))
    }

    async fn revoke(&self, credential: &Credential) -> Result<(), AuthError> {
        self.client
            .close_listen_key(&self.credentials, &credential.value)
            .await
            .map_err(|error| AuthError::Revoke(error.to_string()))
    }
}
