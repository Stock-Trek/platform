use crate::{
    clients::reqwest::ReqwestHttpClient, connector::Connector, submission::SubmissionOutcome,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        exchange::BinanceExchange,
        portfolio::{
            credential_client::BinanceListenKeyClient, listen_key::BinanceListenKeyRequest,
        },
    },
    exchange::ETExchange,
    portfolio::AuthError,
    request::ETHttpRequest,
};
use std::time::Duration;

pub(super) struct BinancePortfolioClient {
    pub(super) exchange: BinanceExchange,
    pub(super) rest_url: String,
    pub(super) request_timeout: Duration,
    pub(super) max_retries: u8,
}

impl BinancePortfolioClient {
    pub(super) fn new(
        rest_url: impl Into<String>,
        request_timeout: Duration,
        max_retries: u8,
    ) -> Self {
        Self {
            exchange: BinanceExchange,
            rest_url: rest_url.into(),
            request_timeout,
            max_retries,
        }
    }

    fn http_connector(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Connector<BinanceExchange, ReqwestHttpClient>> {
        Connector::try_new_http_reqwest(
            self.rest_url.clone(),
            self.exchange,
            BinanceExchange::signer(credentials)?,
            self.request_timeout,
            self.max_retries,
        )
    }

    pub(super) async fn submit<Request>(
        connector: &Connector<BinanceExchange, ReqwestHttpClient>,
        request: Request,
    ) -> BuildResult<Request::Response>
    where
        Request: ETHttpRequest<Exchange = BinanceExchange> + Send,
    {
        match connector.submit_http(request)?.wait().await? {
            SubmissionOutcome::Submitted(response) => Ok(response),
            SubmissionOutcome::Unknown { .. } => Err(platform_errors::BuildError::Exchange(
                platform_errors::ExchangeError::BadResponse(
                    "portfolio request outcome is unknown".to_string(),
                ),
            )),
        }
    }
}

#[async_trait]
impl BinanceListenKeyClient for BinancePortfolioClient {
    async fn create_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> Result<String, AuthError> {
        let connector = self
            .http_connector(credentials)
            .map_err(|error| AuthError::Acquire(error.to_string()))?;
        let response = Self::submit(&connector, BinanceListenKeyRequest::create())
            .await
            .map_err(|error| AuthError::Acquire(error.to_string()))?;
        BinanceExchange::listen_key(response).map_err(|error| AuthError::Acquire(error.to_string()))
    }

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> Result<(), AuthError> {
        let connector = self
            .http_connector(credentials)
            .map_err(|error| AuthError::Renew(error.to_string()))?;
        let response = Self::submit(&connector, BinanceListenKeyRequest::keepalive(listen_key))
            .await
            .map_err(|error| AuthError::Renew(error.to_string()))?;
        BinanceExchange::listen_key_ack(response)
            .map_err(|error| AuthError::Renew(error.to_string()))
    }

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> Result<(), AuthError> {
        let connector = self
            .http_connector(credentials)
            .map_err(|error| AuthError::Revoke(error.to_string()))?;
        let response = Self::submit(&connector, BinanceListenKeyRequest::close(listen_key))
            .await
            .map_err(|error| AuthError::Revoke(error.to_string()))?;
        BinanceExchange::listen_key_ack(response)
            .map_err(|error| AuthError::Revoke(error.to_string()))
    }
}
