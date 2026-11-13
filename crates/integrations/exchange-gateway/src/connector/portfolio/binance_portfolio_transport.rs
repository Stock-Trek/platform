use crate::{
    connector::{
        Connector,
        portfolio::{
            GatewayAccountEventStream, PortfolioEventStream, PortfolioTransport,
            binance_portfolio_client::BinancePortfolioClient,
        },
    },
    iris,
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        exchange::BinanceExchange,
        portfolio::{
            auth_binder::BinanceAuthBinder, credential_provider::BinanceCredentialProvider,
            session_lifecycle::BinanceSessionLifecyclePolicy,
        },
    },
    encode::ByteEncoder,
    exchange::ETExchange,
    exchange_portfolio::ETExchangePortfolio,
    portfolio::{AccountSnapshot, ConnectRequest, ExchangeAdapter},
    signer::Signer,
};
use platform_type_bridge::TickerMapping;
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u8 = 3;

pub struct BinancePortfolioTransport {
    rest_url: String,
    websocket_url: String,
    refresh_interval: Duration,
    reconnect_backoff: Duration,
    mapping: &'static TickerMapping,
}

impl BinancePortfolioTransport {
    pub fn new(
        rest_url: impl Into<String>,
        websocket_url: impl Into<String>,
        refresh_interval: Duration,
        reconnect_backoff: Duration,
        mapping: &'static TickerMapping,
    ) -> Self {
        Self {
            rest_url: rest_url.into(),
            websocket_url: websocket_url.into().trim_end_matches('/').to_string(),
            refresh_interval,
            reconnect_backoff,
            mapping,
        }
    }

    fn http_connector(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Connector<BinanceExchange, crate::clients::reqwest::ReqwestHttpClient>> {
        Connector::try_new_http_reqwest(
            self.rest_url.clone(),
            BinanceExchange,
            BinanceExchange::signer(credentials)?,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )
    }
}

#[async_trait]
impl PortfolioTransport for BinancePortfolioTransport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    fn connect_request(&self) -> BuildResult<ConnectRequest> {
        let url = Url::parse(&self.websocket_url).map_err(|error| {
            BuildError::Exchange(ExchangeError::BadResponse(format!(
                "invalid binance websocket url: {error}"
            )))
        })?;
        Ok(ConnectRequest::new(url))
    }

    async fn adapter(&self, credentials: &ApiKeyCredentials) -> BuildResult<ExchangeAdapter> {
        let client =
            BinancePortfolioClient::new(self.rest_url.clone(), REQUEST_TIMEOUT, MAX_RETRIES);
        let provider = BinanceCredentialProvider::new(client, credentials.clone());
        let binder = BinanceAuthBinder::new();
        let lifecycle = BinanceSessionLifecyclePolicy::for_refresh_interval(self.refresh_interval);
        Ok(ExchangeAdapter::new(
            Box::new(provider),
            Box::new(binder),
            Box::new(lifecycle),
            Box::new(BinanceExchange::signer(credentials)?),
        ))
    }

    async fn account_snapshot(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        let connector = self.http_connector(credentials)?;
        let response =
            BinancePortfolioClient::submit(&connector, BinanceExchange::account_snapshot_request())
                .await?;
        let mapping = self.mapping;
        BinanceExchange::account_snapshot(response, &|asset| {
            GatewayAccountEventStream::<BinanceExchange>::asset_id(mapping, asset)
        })
    }

    async fn open_stream(
        &self,
        request: ConnectRequest,
    ) -> BuildResult<Box<dyn PortfolioEventStream>> {
        let connector = Connector::try_new_websocket_iris(
            request.url.to_string(),
            BinanceExchange,
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
