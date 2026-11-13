use crate::{
    clients::reqwest::ReqwestHttpClient,
    connector::{
        Connector,
        portfolio::{GatewayAccountEventStream, PortfolioEventStream, PortfolioTransport},
    },
    iris,
    submission::SubmissionOutcome,
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    encode::ByteEncoder,
    exchange::ETExchange,
    exchange_portfolio::{ETExchangePortfolio, PortfolioAuthContext},
    exchange_server_time::{ETExchangeServerTimeHttp, ETExchangeServerTimeWebsocket},
    portfolio::{AccountSnapshot, ConnectRequest, ExchangeAdapter},
    request::ETHttpRequest,
    signer::Signer,
};
use platform_type_bridge::TickerMapping;
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::cex_id::CexId;
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u8 = 3;

pub struct ExchangePortfolioTransport<Exchange> {
    exchange: Exchange,
    rest_url: String,
    websocket_url: String,
    refresh_interval: Duration,
    reconnect_backoff: Duration,
    mapping: &'static TickerMapping,
}

impl<Exchange> ExchangePortfolioTransport<Exchange> {
    pub fn new(
        exchange: Exchange,
        rest_url: impl Into<String>,
        websocket_url: impl Into<String>,
        refresh_interval: Duration,
        reconnect_backoff: Duration,
        mapping: &'static TickerMapping,
    ) -> Self {
        Self {
            exchange,
            rest_url: rest_url.into(),
            websocket_url: websocket_url.into().trim_end_matches('/').to_string(),
            refresh_interval,
            reconnect_backoff,
            mapping,
        }
    }
}

impl<Exchange> ExchangePortfolioTransport<Exchange>
where
    Exchange: ETExchange + ETExchangeServerTimeHttp + Clone + Send + Sync + 'static,
{
    fn http_connector(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Connector<Exchange, ReqwestHttpClient>> {
        Connector::try_new_http_reqwest(
            self.rest_url.clone(),
            self.exchange.clone(),
            Exchange::signer(credentials)?,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )
    }

    async fn submit<Request>(
        connector: &Connector<Exchange, ReqwestHttpClient>,
        request: Request,
    ) -> BuildResult<Request::Response>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send,
    {
        match connector.submit_http(request)?.wait().await? {
            SubmissionOutcome::Submitted(response) => Ok(response),
            SubmissionOutcome::Unknown { .. } => Err(BuildError::Exchange(
                ExchangeError::BadResponse("portfolio request outcome is unknown".to_string()),
            )),
        }
    }
}

#[async_trait]
impl<Exchange> PortfolioTransport for ExchangePortfolioTransport<Exchange>
where
    Exchange: ETExchange
        + ETExchangeServerTimeHttp
        + ETExchangeServerTimeWebsocket
        + ETExchangePortfolio
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn cex_id(&self) -> CexId {
        self.exchange.cex_id()
    }

    fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    fn connect_request(&self) -> BuildResult<ConnectRequest> {
        let url = Url::parse(&self.websocket_url).map_err(|error| {
            BuildError::Exchange(ExchangeError::BadResponse(format!(
                "invalid websocket url: {error}"
            )))
        })?;
        Ok(ConnectRequest::new(url))
    }

    async fn adapter(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Box<dyn ExchangeAdapter>> {
        let http = Arc::new(ReqwestHttpClient::try_new(&self.rest_url)?);
        let context = PortfolioAuthContext::new(
            credentials.clone(),
            http,
            self.rest_url.clone(),
            self.refresh_interval,
        );
        Ok(Box::new(self.exchange.adapter(context)?))
    }

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, Exchange::account_snapshot_request()).await?;
        let mapping = self.mapping;
        Exchange::account_snapshot(response, &|asset| {
            GatewayAccountEventStream::<Exchange>::asset_id(mapping, asset)
        })
    }

    async fn open_stream(
        &self,
        request: ConnectRequest,
    ) -> BuildResult<Box<dyn PortfolioEventStream>> {
        let connector = Connector::try_new_websocket_iris(
            request.url.to_string(),
            self.exchange.clone(),
            Signer::new_unencrypted(ByteEncoder::HexLower),
            iris::Config::new(),
            REQUEST_TIMEOUT,
            0,
        )?;
        let listener = connector
            .websocket_listener
            .as_ref()
            .ok_or_else(|| {
                BuildError::Exchange(ExchangeError::BadResponse(
                    "websocket listener is missing".to_string(),
                ))
            })?
            .clone();
        let events = listener.subscribe();
        connector.connect().await?;
        Ok(Box::new(GatewayAccountEventStream::new(
            connector,
            events,
            self.mapping,
        )))
    }
}
