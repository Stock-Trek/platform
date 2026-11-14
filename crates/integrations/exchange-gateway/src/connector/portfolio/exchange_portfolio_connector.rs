use crate::connector::portfolio::{
    ConnectorHealthSink, ExchangePortfolioFeed, PortfolioConnector, PortfolioCredentialSource,
    PortfolioExchange, PortfolioFeed, PortfolioUpdateSink,
    exchange_portfolio_connection::ExchangePortfolioConnection, health_reporter::HealthReporter,
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use std::{collections::BTreeMap, sync::Arc};
use stock_trek_types::cex::cex_id::CexId;

pub struct ExchangePortfolioConnector {
    exchanges: BTreeMap<CexId, Arc<dyn PortfolioExchange>>,
    credentials: Arc<dyn PortfolioCredentialSource>,
    health: Arc<dyn ConnectorHealthSink>,
}

impl ExchangePortfolioConnector {
    pub fn new(
        exchanges: impl IntoIterator<Item = Arc<dyn PortfolioExchange>>,
        credentials: Arc<dyn PortfolioCredentialSource>,
        health: Arc<dyn ConnectorHealthSink>,
    ) -> Self {
        let exchanges = exchanges
            .into_iter()
            .map(|exchange| (exchange.cex_id(), exchange))
            .collect();
        Self {
            exchanges,
            credentials,
            health,
        }
    }

    pub fn feed(self) -> Arc<dyn PortfolioFeed> {
        Arc::new(ExchangePortfolioFeed::new(Arc::new(self)))
    }

    fn reporter(&self, cex_id: CexId) -> HealthReporter {
        HealthReporter::new(cex_id, Arc::clone(&self.health))
    }
}

#[async_trait]
impl PortfolioConnector for ExchangePortfolioConnector {
    async fn connect(
        &self,
        cex_id: CexId,
        sink: Arc<dyn PortfolioUpdateSink>,
    ) -> PlatformResult<Arc<dyn crate::connector::portfolio::PortfolioConnection>> {
        let Some(exchange) = self.exchanges.get(&cex_id) else {
            return Err(PlatformError::bad_value(
                "portfolio connector",
                cex_id.to_string(),
                "is not supported",
            ));
        };
        let connection = Arc::new(ExchangePortfolioConnection::new(
            cex_id,
            Arc::clone(exchange),
            Arc::clone(&self.credentials),
            sink,
            self.reporter(cex_id),
        ));
        connection.connecting().await;
        Ok(connection)
    }
}
