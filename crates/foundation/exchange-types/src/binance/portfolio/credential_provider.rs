use crate::{
    binance::portfolio::credential_client::BinanceListenKeyClient,
    portfolio::{AuthError, Credential, CredentialProvider},
};
use async_trait::async_trait;
use std::time::{Duration, Instant};

pub struct BinanceCredentialProvider {
    client: BinanceListenKeyClient,
    ttl: Duration,
}

impl BinanceCredentialProvider {
    pub fn new(client: BinanceListenKeyClient) -> Self {
        Self {
            client,
            ttl: Duration::from_mins(60),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    fn credential(&self, value: String) -> Credential {
        let issued_at = Instant::now();
        Credential::new(value, issued_at, Some(issued_at + self.ttl))
    }
}

#[async_trait]
impl CredentialProvider for BinanceCredentialProvider {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError> {
        self.client
            .create()
            .await
            .map(|value| Some(self.credential(value)))
    }

    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError> {
        self.client.keepalive(&credential.value).await?;
        Ok(self.credential(credential.value.clone()))
    }

    async fn revoke(&self, credential: &Credential) -> Result<(), AuthError> {
        self.client.close(&credential.value).await
    }
}
