use super::{
    balances::BalanceParser,
    config::BinancePortfolioConfig,
    event_parser::EventParser,
    listen_key::BinanceListenKeyRequest,
    transport::{AccountSnapshot, BinanceAccountEvent, BinanceEventStream, BinanceTransport},
};
use crate::{
    clients::reqwest::ReqwestHttpClient, connector::Connector, iris, submission::SubmissionOutcome,
    websocket_listener::WebsocketEventStream,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        account_status::BinanceAccountStatusRequest,
        exchange::BinanceExchange,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    encode::ByteEncoder,
    request::ETHttpRequest,
    signer::Signer,
};
use platform_type_bridge::{BinanceConverter, TickerMapping};
use std::{collections::BTreeMap, time::Duration};
use stock_trek_types::cex::asset_id::AssetId;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RETRIES: u8 = 3;

pub struct GatewayBinanceTransport {
    config: BinancePortfolioConfig,
    mapping: &'static TickerMapping,
}

impl GatewayBinanceTransport {
    pub fn new(config: BinancePortfolioConfig) -> Self {
        Self {
            config,
            mapping: BinanceConverter::ticker_mapping_static(),
        }
    }

    fn http_connector(
        &self,
        credentials: &ApiKeyCredentials,
    ) -> BpResult<Connector<BinanceExchange, ReqwestHttpClient>> {
        Connector::try_new_http_reqwest(
            self.config.rest_url().to_string(),
            BinanceExchange,
            BinanceExchange::signer_hmac_sha256(credentials.clone())?,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )
    }

    async fn submit<Request>(
        connector: &Connector<BinanceExchange, ReqwestHttpClient>,
        request: Request,
    ) -> BpResult<Request::Response>
    where
        Request: ETHttpRequest<Exchange = BinanceExchange> + Send,
    {
        match connector.submit_http(request)?.wait().await? {
            SubmissionOutcome::Submitted(response) => Ok(response),
            SubmissionOutcome::Unknown { .. } => Err(BpError::Exchange(
                ExchangeError::BadResponse("listen key request outcome is unknown".to_string()),
            )),
        }
    }

    fn success<R>(response: BinanceResponse<R>) -> BpResult<R> {
        match response.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => Err(BpError::Exchange(
                ExchangeError::BadResponse(error.to_string()),
            )),
        }
    }

    fn balances(
        &self,
        balances: impl IntoIterator<Item = (String, String, String)>,
    ) -> BpResult<BTreeMap<AssetId, f64>> {
        BalanceParser::parse(self.mapping, balances)
    }
}

#[async_trait]
impl BinanceTransport for GatewayBinanceTransport {
    async fn create_listen_key(&self, credentials: &ApiKeyCredentials) -> BpResult<String> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, BinanceListenKeyRequest::create()).await?;
        Self::success(response)?.require_listen_key()
    }

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()> {
        let connector = self.http_connector(credentials)?;
        let response =
            Self::submit(&connector, BinanceListenKeyRequest::keepalive(listen_key)).await?;
        Self::success(response).map(|_| ())
    }

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, BinanceListenKeyRequest::close(listen_key)).await?;
        Self::success(response).map(|_| ())
    }

    async fn account_snapshot(&self, credentials: &ApiKeyCredentials) -> BpResult<AccountSnapshot> {
        let connector = self.http_connector(credentials)?;
        let response = Self::submit(&connector, BinanceAccountStatusRequest::new()).await?;
        let account = Self::success(response)?;
        let occurred_at = Utc
            .timestamp_millis_opt(account.updateTime)
            .single()
            .ok_or_else(|| {
                BpError::Exchange(ExchangeError::BadResponse(
                    "invalid account timestamp".to_string(),
                ))
            })?;
        let balances = self.balances(account.balances.into_iter().map(|balance| {
            (
                balance.asset,
                balance.free.to_string(),
                balance.locked.to_string(),
            )
        }))?;
        Ok(AccountSnapshot::new(balances, occurred_at))
    }

    async fn open_stream(&self, listen_key: &str) -> BpResult<Box<dyn BinanceEventStream>> {
        let url = format!("{}/{listen_key}", self.config.websocket_url());
        let connector = Connector::try_new_websocket_iris(
            url,
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
                BpError::Exchange(ExchangeError::BadResponse(
                    "websocket listener is missing".to_string(),
                ))
            })?
            .clone();
        let events = listener.subscribe();
        connector.connect().await?;
        Ok(Box::new(GatewayEventStream {
            _connector: connector,
            events,
            mapping: self.mapping,
        }))
    }
}

struct GatewayEventStream {
    _connector: Connector<BinanceExchange, crate::clients::iris::IrisWebsocketClient>,
    events: WebsocketEventStream,
    mapping: &'static TickerMapping,
}

#[async_trait]
impl BinanceEventStream for GatewayEventStream {
    async fn next(&mut self) -> BpResult<Option<BinanceAccountEvent>> {
        loop {
            let Some(value) = self.events.next().await else {
                return Ok(None);
            };
            if let Some(event) = EventParser::parse(&value, self.mapping)? {
                return Ok(Some(event));
            }
        }
    }
}
