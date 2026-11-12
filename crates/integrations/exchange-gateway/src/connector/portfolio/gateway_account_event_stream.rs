use crate::connector::portfolio::AccountEventStream;
use crate::connector::portfolio::GatewayPortfolioTransport;
use crate::{
    clients::iris::IrisWebsocketClient, connector::Connector,
    websocket_listener::WebsocketEventStream,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_types::portfolio::{AccountEvent, PortfolioExchange};
use platform_type_bridge::TickerMapping;

pub struct GatewayAccountEventStream<Exchange> {
    _connector: Connector<Exchange, IrisWebsocketClient>,
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
            _connector: connector,
            events,
            mapping,
        }
    }
}

#[async_trait]
impl<Exchange> AccountEventStream for GatewayAccountEventStream<Exchange>
where
    Exchange: PortfolioExchange + Send + Sync + 'static,
{
    async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        loop {
            let Some(value) = self.events.next().await else {
                return Ok(None);
            };
            let mapping = self.mapping;
            if let Some(event) = Exchange::account_event(&value, &|asset| {
                GatewayPortfolioTransport::<Exchange>::asset_id(mapping, asset)
            })? {
                return Ok(Some(event));
            }
        }
    }
}
