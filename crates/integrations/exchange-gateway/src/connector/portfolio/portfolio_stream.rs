use iris::{Client, Listener};
use platform_errors::BuildResult;
use platform_exchange_types::portfolio::AccountEvent;
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::mpsc;

pub type PortfolioClient = Client<serde_json::Value, serde_json::Value>;

pub enum PortfolioEvent {
    Account(AccountEvent),
    Closed,
    Failed(String),
}

pub struct PortfolioStream {
    client: PortfolioClient,
    events: mpsc::UnboundedReceiver<PortfolioEvent>,
}

impl PortfolioStream {
    pub fn new(client: PortfolioClient, events: mpsc::UnboundedReceiver<PortfolioEvent>) -> Self {
        Self { client, events }
    }

    pub fn client(&self) -> &PortfolioClient {
        &self.client
    }

    pub fn listener<Exchange>(
        events: mpsc::UnboundedSender<PortfolioEvent>,
        mapping: &'static platform_type_bridge::TickerMapping,
    ) -> Arc<dyn Listener<serde_json::Value>>
    where
        Exchange: platform_exchange_types::exchange_portfolio::ETExchangePortfolio
            + Send
            + Sync
            + 'static,
    {
        Arc::new(PortfolioListener::<Exchange> {
            events,
            mapping,
            marker: PhantomData,
        })
    }

    pub async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        match self.events.recv().await {
            Some(PortfolioEvent::Account(event)) => Ok(Some(event)),
            Some(PortfolioEvent::Closed) | None => Ok(None),
            Some(PortfolioEvent::Failed(error)) => Err(platform_errors::BuildError::external(
                std::io::Error::other(error),
            )),
        }
    }

    pub async fn disconnect(&self) -> BuildResult<()> {
        self.client
            .disconnect()
            .await
            .map_err(platform_errors::BuildError::external)
    }
}

struct PortfolioListener<Exchange> {
    events: mpsc::UnboundedSender<PortfolioEvent>,
    mapping: &'static platform_type_bridge::TickerMapping,
    marker: PhantomData<fn() -> Exchange>,
}

impl<Exchange> std::fmt::Debug for PortfolioListener<Exchange> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortfolioListener")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<Exchange> Listener<serde_json::Value> for PortfolioListener<Exchange>
where
    Exchange:
        platform_exchange_types::exchange_portfolio::ETExchangePortfolio + Send + Sync + 'static,
{
    async fn on_message(&self, value: serde_json::Value) {
        let mapping = self.mapping;
        match Exchange::account_event(&value, &|asset| {
            mapping
                .asset_id(&platform_exchange_types::ticker::Ticker(asset.to_string()))
                .copied()
        }) {
            Ok(Some(event)) => {
                let _ = self.events.send(PortfolioEvent::Account(event));
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "failed to parse portfolio account event");
            }
        }
    }

    async fn on_disconnected(&self) {
        let _ = self.events.send(PortfolioEvent::Closed);
    }

    async fn on_error(&self, error: &iris::ConnectionError) {
        let _ = self.events.send(PortfolioEvent::Failed(format!(
            "portfolio websocket failed: {error}"
        )));
    }
}
