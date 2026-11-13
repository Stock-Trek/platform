use crate::clients::client::WebsocketClient;
use crate::connector::portfolio::AccountEventStream;
use crate::{
    clients::iris::IrisWebsocketClient, connector::Connector,
    websocket_listener::WebsocketEventStream,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_types::exchange_portfolio::ETExchangePortfolio;
use platform_exchange_types::{
    exchange::ETExchange,
    portfolio::{AccountEvent, AuthError, WsConnection},
    ticker::Ticker,
};
use platform_type_bridge::TickerMapping;
use std::time::Duration;
use stock_trek_types::cex::asset_id::AssetId;

const SEND_TIMEOUT: Duration = Duration::from_secs(10);

pub struct GatewayAccountEventStream<Exchange> {
    connector: Connector<Exchange, IrisWebsocketClient>,
    events: WebsocketEventStream,
    mapping: &'static TickerMapping,
}

impl<Exchange> GatewayAccountEventStream<Exchange> {
    pub(super) fn new(
        connector: Connector<Exchange, IrisWebsocketClient>,
        events: WebsocketEventStream,
        mapping: &'static TickerMapping,
    ) -> Self {
        Self {
            connector,
            events,
            mapping,
        }
    }

    pub(super) fn asset_id(mapping: &TickerMapping, asset: &str) -> Option<AssetId> {
        mapping.asset_id(&Ticker(asset.to_string())).copied()
    }
}

#[async_trait]
impl<Exchange> AccountEventStream for GatewayAccountEventStream<Exchange>
where
    Exchange: ETExchange + ETExchangePortfolio + Send + Sync + 'static,
{
    async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        loop {
            let Some(value) = self.events.next().await else {
                return Ok(None);
            };
            let mapping = self.mapping;
            if let Some(event) =
                Exchange::account_event(&value, &|asset| Self::asset_id(mapping, asset))?
            {
                return Ok(Some(event));
            }
        }
    }
}

#[async_trait]
impl<Exchange> WsConnection for GatewayAccountEventStream<Exchange>
where
    Exchange: Send + Sync + 'static,
{
    async fn send_text(&self, payload: &str) -> Result<(), AuthError> {
        self.connector
            .client
            .send(payload.to_string(), SEND_TIMEOUT)
            .await
            .map_err(|error| AuthError::Transport(error.to_string()))
    }

    async fn send_binary(&self, payload: &[u8]) -> Result<(), AuthError> {
        let payload = std::str::from_utf8(payload)
            .map_err(|error| AuthError::Transport(error.to_string()))?;
        self.send_text(payload).await
    }
}
