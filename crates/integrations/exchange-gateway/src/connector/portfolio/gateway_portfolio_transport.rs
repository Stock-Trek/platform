use crate::{
    clients::reqwest::ReqwestHttpClient,
    connector::{
        Connector,
        portfolio::{
            AccountEventStream, PortfolioTransport,
            gateway_account_event_stream::GatewayAccountEventStream,
        },
    },
    iris,
    submission::SubmissionOutcome,
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_exchange_types::exchange_server_time::{
    ETExchangeServerTimeHttp, ETExchangeServerTimeWebsocket,
};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials, encode::ByteEncoder, exchange::ETExchange,
    exchange_portfolio::ETExchangePortfolio, portfolio::AccountSnapshot, request::ETHttpRequest,
    signer::Signer, ticker::Ticker,
};
use platform_type_bridge::TickerMapping;
use std::time::Duration;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u8 = 3;

pub struct GatewayPortfolioTransport<Exchange> {
    exchange: Exchange,
    rest_url: String,
    websocket_url: String,
    refresh_interval: Duration,
    reconnect_backoff: Duration,
    mapping: &'static TickerMapping,
}

impl<Exchange> GatewayPortfolioTransport<Exchange> {
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

    pub(super) fn asset_id(mapping: &TickerMapping, asset: &str) -> Option<AssetId> {
        mapping.asset_id(&Ticker(asset.to_string())).copied()
    }
}

impl<Exchange> GatewayPortfolioTransport<Exchange>
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
impl<Exchange> PortfolioTransport for GatewayPortfolioTransport<Exchange>
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

    fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    async fn create_listen_key(&self, credentials: &ApiKeyCredentials) -> BuildResult<String> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, Exchange::listen_key_request()).await?;
        Exchange::listen_key(response)
    }

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BuildResult<()> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(
            &connector,
            Exchange::keepalive_listen_key_request(listen_key),
        )
        .await?;
        Exchange::listen_key_ack(response)
    }

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BuildResult<()> {
        let connector = self.http_connector(credentials)?;
        let response =
            Self::submit(&connector, Exchange::close_listen_key_request(listen_key)).await?;
        Exchange::listen_key_ack(response)
    }

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, Exchange::account_snapshot_request()).await?;
        let mapping = self.mapping;
        Exchange::account_snapshot(response, &|asset| Self::asset_id(mapping, asset))
    }

    async fn open_stream(&self, listen_key: &str) -> BuildResult<Box<dyn AccountEventStream>> {
        let url = format!("{}/{listen_key}", self.websocket_url);
        let connector = Connector::try_new_websocket_iris(
            url,
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
