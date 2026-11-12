use super::{PortfolioConnection, PortfolioConnector, PortfolioFeed, PortfolioUpdateSink};
use async_trait::async_trait;
use parking_lot::RwLock;
use platform_errors::{BpError, BpResult};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};
use tokio::sync::Mutex;

pub struct ExchangePortfolioFeed {
    connector: Arc<dyn PortfolioConnector>,
    connections: Mutex<BTreeMap<CexId, Arc<dyn PortfolioConnection>>>,
    update_sink: RwLock<Option<Arc<dyn PortfolioUpdateSink>>>,
}

impl ExchangePortfolioFeed {
    pub fn new(connector: Arc<dyn PortfolioConnector>) -> Self {
        Self {
            connector,
            connections: Mutex::new(BTreeMap::new()),
            update_sink: RwLock::new(None),
        }
    }

    pub async fn connection_count(&self) -> usize {
        self.connections.lock().await.len()
    }

    fn update_sink(&self) -> BpResult<Arc<dyn PortfolioUpdateSink>> {
        self.update_sink
            .read()
            .clone()
            .ok_or_else(|| BpError::bad_value("portfolio update sink", "", "is not configured"))
    }
}

#[async_trait]
impl PortfolioFeed for ExchangePortfolioFeed {
    async fn set_accounts(&self, cex_id: CexId, accounts: BTreeSet<AccountId>) -> BpResult<()> {
        let mut connections = self.connections.lock().await;
        if accounts.is_empty() {
            if let Some(connection) = connections.remove(&cex_id) {
                connection.disconnect().await?;
            }
            return Ok(());
        }
        let connection = match connections.get(&cex_id) {
            Some(connection) => Arc::clone(connection),
            None => {
                let connection = self.connector.connect(cex_id, self.update_sink()?).await?;
                connections.insert(cex_id, Arc::clone(&connection));
                connection
            }
        };
        connection.update_accounts(accounts).await
    }

    fn set_update_sink(&self, sink: Arc<dyn PortfolioUpdateSink>) {
        *self.update_sink.write() = Some(sink);
    }
}
