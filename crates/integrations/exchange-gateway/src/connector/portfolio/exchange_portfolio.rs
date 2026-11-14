use crate::{
    clients::reqwest::ReqwestHttpClient,
    connector::{
        Connector,
        portfolio::{PortfolioExchange, PortfolioStream},
    },
    submission::SubmissionOutcome,
};
use async_trait::async_trait;
use platform_errors::{ExchangeError, PlatformError, PlatformResult};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    exchange::ETExchange,
    exchange_account_snapshot::AccountSnapshotContext,
    exchange_portfolio::{ETExchangePortfolio, PortfolioAuthContext},
    exchange_server_time::{ETExchangeServerTimeHttp, ETExchangeServerTimeWebsocket},
    portfolio::{AccountSnapshot, Credential, PortfolioAuth},
    request::ETHttpRequest,
};
use platform_type_bridge::TickerMapping;
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::cex_id::CexId;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u8 = 3;

pub struct ExchangePortfolio<Exchange> {
    exchange: Exchange,
    rest_url: String,
    websocket_url: String,
    refresh_interval: Duration,
    reconnect_backoff: Duration,
    mapping: &'static TickerMapping,
}

impl<Exchange> ExchangePortfolio<Exchange> {
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

impl<Exchange> ExchangePortfolio<Exchange>
where
    Exchange: ETExchange + ETExchangeServerTimeHttp + Clone + Send + Sync + 'static,
{
    fn http_connector(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> PlatformResult<Connector<Exchange, ReqwestHttpClient>> {
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
    ) -> PlatformResult<Request::Response>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send,
    {
        match connector.submit_http(request)?.wait().await? {
            SubmissionOutcome::Submitted(response) => Ok(response),
            SubmissionOutcome::Unknown { .. } => Err(PlatformError::Exchange(
                ExchangeError::BadResponse("portfolio request outcome is unknown".to_string()),
            )),
        }
    }
}

#[async_trait]
impl<Exchange> PortfolioExchange for ExchangePortfolio<Exchange>
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

    async fn portfolio_auth(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> PlatformResult<PortfolioAuth> {
        let http = Arc::new(ReqwestHttpClient::try_new(&self.rest_url)?);
        let context = PortfolioAuthContext::new(
            credentials.clone(),
            http,
            self.rest_url.clone(),
            self.refresh_interval,
        );
        self.exchange.portfolio_auth(context)
    }

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> PlatformResult<AccountSnapshot> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, Exchange::account_snapshot_request()).await?;
        let mapping = self.mapping;
        let asset_id = |asset: &str| {
            mapping
                .asset_id(&platform_exchange_types::ticker::Ticker(asset.to_string()))
                .copied()
        };
        let context = AccountSnapshotContext::new(&asset_id);
        Exchange::account_snapshot(response, &context)
    }

    async fn open_stream(
        &self,
        auth: &PortfolioAuth,
        credential: Option<&Credential>,
    ) -> PlatformResult<PortfolioStream> {
        let binder = auth.auth_binder();
        let url = binder.websocket_url(&self.websocket_url, credential, auth.signer())?;
        let config = binder.configure(iris::Config::new(), credential, auth.signer())?;
        let (sender, events) = tokio::sync::mpsc::unbounded_channel();
        let listener = PortfolioStream::listener::<Exchange>(sender, self.mapping);
        let client = iris::Client::new(config, listener, &url);
        client.connect().await.map_err(PlatformError::external)?;
        Ok(PortfolioStream::new(client, events))
    }
}
