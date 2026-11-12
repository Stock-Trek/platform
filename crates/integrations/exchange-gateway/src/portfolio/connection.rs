use super::{
    ConnectorHealthSink, ConnectorState, ExchangePortfolioFeed, PortfolioConnection,
    PortfolioConnector, PortfolioCredentialSource, PortfolioExchangeTransport, PortfolioFeed,
    PortfolioUpdateSink, health_reporter::HealthReporter, watcher::Watcher,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_metrics::PortfolioMetrics;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::timeout,
};

pub struct ExchangePortfolioConnector {
    transports: BTreeMap<CexId, Arc<dyn PortfolioExchangeTransport>>,
    credentials: Arc<dyn PortfolioCredentialSource>,
    health: Arc<dyn ConnectorHealthSink>,
}

impl ExchangePortfolioConnector {
    pub fn new(
        transports: impl IntoIterator<Item = Arc<dyn PortfolioExchangeTransport>>,
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
    ) -> BpResult<Arc<dyn PortfolioConnection>> {
        let Some(transport) = self.transports.get(&cex_id) else {
            return Err(BpError::bad_value(
                "portfolio connector",
                cex_id.to_string(),
                "is not supported",
            ));
        };
        let connection = Arc::new(ExchangePortfolioConnection {
            cex_id,
            transport: Arc::clone(transport),
            credentials: Arc::clone(&self.credentials),
            sink,
            health: self.reporter(cex_id),
            watchers: Mutex::new(BTreeMap::new()),
        });
        connection
            .health
            .publish(ConnectorState::Connecting, 0, None)
            .await;
        Ok(connection)
    }
}

struct AccountWatcher {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct ExchangePortfolioConnection {
    cex_id: CexId,
    transport: Arc<dyn PortfolioExchangeTransport>,
    credentials: Arc<dyn PortfolioCredentialSource>,
    sink: Arc<dyn PortfolioUpdateSink>,
    health: HealthReporter,
    watchers: Mutex<BTreeMap<AccountId, AccountWatcher>>,
}

impl ExchangePortfolioConnection {
    async fn stop(watcher: AccountWatcher) {
        let AccountWatcher { shutdown, mut task } = watcher;
        let _ = shutdown.send(true);
        if timeout(Duration::from_secs(2), &mut task).await.is_err() {
            task.abort();
        }
    }

    fn spawn(&self, account: AccountId) -> AccountWatcher {
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(
            Watcher {
                cex_id: self.cex_id,
                account,
                transport: Arc::clone(&self.transport),
                credentials: Arc::clone(&self.credentials),
                sink: Arc::clone(&self.sink),
                health: self.health.clone(),
                shutdown: receiver,
            }
            .run(),
        );
        AccountWatcher { shutdown, task }
    }
}

#[async_trait]
impl PortfolioConnection for ExchangePortfolioConnection {
    async fn update_accounts(&self, accounts: BTreeSet<AccountId>) -> BpResult<()> {
        let mut watchers = self.watchers.lock().await;
        let removed: Vec<AccountId> = watchers
            .keys()
            .filter(|account| !accounts.contains(*account))
            .cloned()
            .collect();
        for account in removed {
            if let Some(watcher) = watchers.remove(&account) {
                Self::stop(watcher).await;
            }
        }
        for account in accounts {
            if let std::collections::btree_map::Entry::Vacant(entry) = watchers.entry(account) {
                let watcher = self.spawn(entry.key().clone());
                entry.insert(watcher);
            }
        }
        let cex = self.cex_id.to_string();
        PortfolioMetrics::record_subscriptions(cex.clone(), watchers.len());
        let count = watchers.len();
        drop(watchers);
        self.health
            .publish(ConnectorState::Connected, count, None)
            .await;
        Ok(())
    }

    async fn disconnect(&self) -> BpResult<()> {
        let watchers = {
            let mut watchers = self.watchers.lock().await;
            std::mem::take(&mut *watchers)
        };
        for watcher in watchers.into_values() {
            Self::stop(watcher).await;
        }
        let cex = self.cex_id.to_string();
        PortfolioMetrics::record_connection_state(cex.clone(), false);
        PortfolioMetrics::record_subscriptions(cex, 0);
        self.health
            .publish(ConnectorState::Disconnected, 0, None)
            .await;
        Ok(())
    }
}
