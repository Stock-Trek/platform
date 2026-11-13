use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{exchange::BinanceExchange, portfolio::listen_key::BinanceListenKeyRequest},
    exchange::ETExchange,
    http::HttpClient,
    portfolio::AuthError,
    request::ETHttpRequest,
    response::ETHttpResponse,
};
use platform_errors::BuildResult;
use std::{sync::Arc, time::Duration};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct BinanceListenKeyClient {
    http: Arc<dyn HttpClient>,
    credentials: ApiKeyCredentials,
}

impl BinanceListenKeyClient {
    pub fn new(http: Arc<dyn HttpClient>, credentials: ApiKeyCredentials) -> Self {
        Self { http, credentials }
    }

    pub async fn create(&self) -> Result<String, AuthError> {
        let response = self
            .send(BinanceListenKeyRequest::create())
            .await
            .map_err(|error| AuthError::Acquire(error.to_string()))?;
        BinanceExchange::listen_key(response).map_err(|error| AuthError::Acquire(error.to_string()))
    }

    pub async fn keepalive(&self, listen_key: &str) -> Result<(), AuthError> {
        let response = self
            .send(BinanceListenKeyRequest::keepalive(listen_key))
            .await
            .map_err(|error| AuthError::Renew(error.to_string()))?;
        BinanceExchange::listen_key_ack(response)
            .map_err(|error| AuthError::Renew(error.to_string()))
    }

    pub async fn close(&self, listen_key: &str) -> Result<(), AuthError> {
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
