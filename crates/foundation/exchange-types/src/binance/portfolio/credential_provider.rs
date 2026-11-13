use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{exchange::BinanceExchange, portfolio::listen_key::BinanceListenKeyRequest},
    exchange::ETExchange,
    http::HttpClient,
    portfolio::{AuthError, Credential, CredentialProvider},
    request::ETHttpRequest,
    response::ETHttpResponse,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct BinanceCredentialProvider {
    http: Arc<dyn HttpClient>,
    credentials: ApiKeyCredentials,
    ttl: Duration,
}

impl BinanceCredentialProvider {
    pub fn new(http: Arc<dyn HttpClient>, credentials: ApiKeyCredentials) -> Self {
        Self {
            http,
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
        Credential::new(value, issued_at, Some(issued_at + self.ttl))
    }

    async fn create(&self) -> Result<String, AuthError> {
        let response = self
            .send(BinanceListenKeyRequest::create())
            .await
            .map_err(|error| AuthError::Acquire(error.to_string()))?;
        BinanceExchange::listen_key(response).map_err(|error| AuthError::Acquire(error.to_string()))
    }

    async fn keepalive(&self, listen_key: &str) -> Result<(), AuthError> {
        let response = self
            .send(BinanceListenKeyRequest::keepalive(listen_key))
            .await
            .map_err(|error| AuthError::Renew(error.to_string()))?;
        BinanceExchange::listen_key_ack(response)
            .map_err(|error| AuthError::Renew(error.to_string()))
    }

    async fn close(&self, listen_key: &str) -> Result<(), AuthError> {
        let response = self
            .send(BinanceListenKeyRequest::close(listen_key))
            .await
            .map_err(|error| AuthError::Revoke(error.to_string()))?;
        BinanceExchange::listen_key_ack(response)
            .map_err(|error| AuthError::Revoke(error.to_string()))
    }

    async fn send<Request>(&self, request: Request) -> BuildResult<Request::Response>
    where
        Request: ETHttpRequest<Exchange = BinanceExchange> + Send,
    {
        let signer = BinanceExchange::signer(&self.credentials)?;
        let http_request = request.try_into_http(&signer)?;
        let response = self.http.send(http_request, REQUEST_TIMEOUT).await?;
        <Request::Response as ETHttpResponse>::try_from_http(response)
    }
}

#[async_trait]
impl CredentialProvider for BinanceCredentialProvider {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError> {
        self.create().await.map(|value| Some(self.credential(value)))
    }

    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError> {
        self.keepalive(&credential.value).await?;
        Ok(self.credential(credential.value.clone()))
    }

    async fn revoke(&self, credential: &Credential) -> Result<(), AuthError> {
        self.close(&credential.value).await
    }
}
