use super::{
    config::BinancePortfolioConfig, credentials::BinanceCredentialSource,
    gateway_transport::GatewayBinanceTransport, health_reporter::HealthReporter,
    session::WatcherConfig, transport::BinanceTransport,
};
use crate::portfolio::{
    ConnectorState, PortfolioConnection, PortfolioConnector, PortfolioUpdateSink,
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

const CEX: &str = "binance";

pub struct BinancePortfolioConnector {
    config: Arc<BinancePortfolioConfig>,
    transport: Arc<dyn BinanceTransport>,
    credentials: Arc<dyn BinanceCredentialSource>,
    health: HealthReporter,
}

impl BinancePortfolioConnector {
    pub fn new(
        config: BinancePortfolioConfig,
        credentials: Arc<dyn BinanceCredentialSource>,
        health: Arc<dyn crate::portfolio::ConnectorHealthSink>,
    ) -> Self {
        let transport = Arc::new(GatewayBinanceTransport::new(config.clone()));
        Self {
            config: Arc::new(config),
            transport,
            credentials,
            health: HealthReporter::new(health),
        }
    }

    pub fn with_transport(mut self, transport: Arc<dyn BinanceTransport>) -> Self {
        self.transport = transport;
        self
    }

    fn connection(&self, sink: Arc<dyn PortfolioUpdateSink>) -> Arc<dyn PortfolioConnection> {
        Arc::new(BinancePortfolioConnection {
            config: Arc::clone(&self.config),
            transport: Arc::clone(&self.transport),
            credentials: Arc::clone(&self.credentials),
            sink,
            health: self.health.clone(),
            watchers: Mutex::new(BTreeMap::new()),
        })
    }
}

#[async_trait]
impl PortfolioConnector for BinancePortfolioConnector {
    async fn connect(
        &self,
        cex_id: CexId,
        sink: Arc<dyn PortfolioUpdateSink>,
    ) -> BpResult<Arc<dyn PortfolioConnection>> {
        if cex_id != CexId::Binance {
            return Err(BpError::bad_value(
                "portfolio connector",
                format!("{cex_id:?}"),
                "only Binance is supported",
            ));
        }
        let connection = self.connection(sink);
        self.health
            .publish(ConnectorState::Connecting, 0, None)
            .await;
        Ok(connection)
    }
}

struct Watcher {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct BinancePortfolioConnection {
    config: Arc<BinancePortfolioConfig>,
    transport: Arc<dyn BinanceTransport>,
    credentials: Arc<dyn BinanceCredentialSource>,
    sink: Arc<dyn PortfolioUpdateSink>,
    health: HealthReporter,
    watchers: Mutex<BTreeMap<AccountId, Watcher>>,
}

impl BinancePortfolioConnection {
    async fn stop(watcher: Watcher) {
        let Watcher { shutdown, mut task } = watcher;
        let _ = shutdown.send(true);
        if timeout(Duration::from_secs(2), &mut task).await.is_err() {
            task.abort();
        }
    }

    fn spawn(&self, account: AccountId) -> Watcher {
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(
            WatcherConfig {
                account,
                config: Arc::clone(&self.config),
                transport: Arc::clone(&self.transport),
                credentials: Arc::clone(&self.credentials),
                sink: Arc::clone(&self.sink),
                health: self.health.clone(),
                shutdown: receiver,
            }
            .run(),
        );
        Watcher { shutdown, task }
    }
}

#[async_trait]
impl PortfolioConnection for BinancePortfolioConnection {
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
        PortfolioMetrics::record_subscriptions(CEX, watchers.len());
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
        PortfolioMetrics::record_connection_state(CEX, false);
        PortfolioMetrics::record_subscriptions(CEX, 0);
        self.health
            .publish(ConnectorState::Disconnected, 0, None)
            .await;
        Ok(())
    }
}
