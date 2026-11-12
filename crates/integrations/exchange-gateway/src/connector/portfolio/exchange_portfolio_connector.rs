use crate::connector::portfolio::PortfolioTransport;
use crate::connector::portfolio::{
    ConnectorHealthSink, ExchangePortfolioFeed, PortfolioConnector, PortfolioCredentialSource,
    PortfolioFeed, PortfolioUpdateSink, exchange_portfolio_connection::ExchangePortfolioConnection,
    health_reporter::HealthReporter,
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult};
use std::{collections::BTreeMap, sync::Arc};
use stock_trek_types::cex::cex_id::CexId;

pub struct ExchangePortfolioConnector {
    transports: BTreeMap<CexId, Arc<dyn PortfolioTransport>>,
    credentials: Arc<dyn PortfolioCredentialSource>,
    health: Arc<dyn ConnectorHealthSink>,
}

impl ExchangePortfolioConnector {
    pub fn new(
        transports: impl IntoIterator<Item = Arc<dyn PortfolioTransport>>,
        credentials: Arc<dyn PortfolioCredentialSource>,
        health: Arc<dyn ConnectorHealthSink>,
    ) -> Self {
        let transports = transports
            .into_iter()
            .map(|transport| (transport.cex_id(), transport))
            .collect();
        Self {
            transports,
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
    ) -> BuildResult<Arc<dyn crate::connector::portfolio::PortfolioConnection>> {
        let Some(transport) = self.transports.get(&cex_id) else {
            return Err(BuildError::bad_value(
                "portfolio connector",
                cex_id.to_string(),
                "is not supported",
            ));
        };
        let connection = Arc::new(ExchangePortfolioConnection::new(
            cex_id,
            Arc::clone(transport),
            Arc::clone(&self.credentials),
            sink,
            self.reporter(cex_id),
        ));
        connection.connecting().await;
        Ok(connection)
    }
}
