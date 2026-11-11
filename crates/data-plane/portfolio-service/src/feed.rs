use crate::PortfolioUpdate;
use async_trait::async_trait;
use parking_lot::RwLock;
use platform_errors::BpResult;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioUpdateApply {
    Applied,
    IgnoredUntracked,
    IgnoredStale,
}

#[async_trait]
pub trait PortfolioUpdateSink: Send + Sync {
    async fn apply_update(&self, update: PortfolioUpdate) -> BpResult<PortfolioUpdateApply>;
}

#[async_trait]
pub trait PortfolioFeed: Send + Sync {
    async fn set_accounts(&self, cex_id: CexId, accounts: BTreeSet<AccountId>) -> BpResult<()>;

    fn set_update_sink(&self, _sink: Arc<dyn PortfolioUpdateSink>) -> BpResult<()> {
        Ok(())
    }
}

#[async_trait]
pub trait PortfolioConnection: Send + Sync {
    async fn update_accounts(&self, accounts: BTreeSet<AccountId>) -> BpResult<()>;

    async fn disconnect(&self) -> BpResult<()>;
}

#[async_trait]
pub trait PortfolioConnector: Send + Sync {
    async fn connect(&self, cex_id: CexId) -> BpResult<Arc<dyn PortfolioConnection>>;

    async fn connect_with_sink(
        &self,
        cex_id: CexId,
        _sink: Arc<dyn PortfolioUpdateSink>,
    ) -> BpResult<Arc<dyn PortfolioConnection>> {
        self.connect(cex_id).await
    }
}

#[derive(Debug, Default)]
pub struct NoopPortfolioFeed;

impl NoopPortfolioFeed {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PortfolioFeed for NoopPortfolioFeed {
    async fn set_accounts(&self, _cex_id: CexId, _accounts: BTreeSet<AccountId>) -> BpResult<()> {
        Ok(())
    }
}

pub struct GroupingPortfolioFeed {
    connector: Arc<dyn PortfolioConnector>,
    connections: Mutex<BTreeMap<CexId, Arc<dyn PortfolioConnection>>>,
    update_sink: RwLock<Option<Arc<dyn PortfolioUpdateSink>>>,
}

impl GroupingPortfolioFeed {
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
}

#[async_trait]
impl PortfolioFeed for GroupingPortfolioFeed {
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
                let sink = self.update_sink.read().clone();
                let connection = match sink {
                    Some(sink) => self.connector.connect_with_sink(cex_id, sink).await?,
                    None => self.connector.connect(cex_id).await?,
                };
                connections.insert(cex_id, Arc::clone(&connection));
                connection
            }
        };
        connection.update_accounts(accounts).await
    }

    fn set_update_sink(&self, sink: Arc<dyn PortfolioUpdateSink>) -> BpResult<()> {
        *self.update_sink.write() = Some(sink);
        Ok(())
    }
}
