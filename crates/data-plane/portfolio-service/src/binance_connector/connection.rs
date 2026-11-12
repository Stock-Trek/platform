use super::{
    credentials::BinanceCredentialSource, health_reporter::HealthReporter, session::WatcherConfig,
    transport::BinanceTransport,
};
use crate::{
    AccountKey,
    feed::{PortfolioConnection, PortfolioConnector, PortfolioUpdateSink},
    health::ConnectorState,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_metrics::PortfolioMetrics;
use platform_nats::EventPublisher;
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
    transport: Arc<dyn BinanceTransport>,
    credentials: Arc<dyn BinanceCredentialSource>,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
    health: HealthReporter,
}

impl BinancePortfolioConnector {
    pub fn new(
        transport: Arc<dyn BinanceTransport>,
        credentials: Arc<dyn BinanceCredentialSource>,
        listen_key_refresh: Duration,
        reconnect_backoff: Duration,
    ) -> Self {
        Self {
            transport,
            credentials,
            listen_key_refresh,
            reconnect_backoff,
            health: HealthReporter::default(),
        }
    }

    pub fn with_health_publisher(
        mut self,
        publisher: Arc<dyn EventPublisher>,
        subject: impl Into<String>,
    ) -> Self {
        self.health = HealthReporter::new(Some(publisher), subject);
        self
    }

    async fn connect_with_sink_optional(
        &self,
        cex_id: CexId,
        sink: Option<Arc<dyn PortfolioUpdateSink>>,
    ) -> BpResult<Arc<dyn PortfolioConnection>> {
        if cex_id != CexId::Binance {
            return Err(BpError::bad_value(
                "portfolio connector",
                format!("{cex_id:?}"),
                "only Binance is supported",
            ));
        }
        let connection = Arc::new(BinancePortfolioConnection {
            transport: Arc::clone(&self.transport),
            credentials: Arc::clone(&self.credentials),
            sink,
            listen_key_refresh: self.listen_key_refresh,
            reconnect_backoff: self.reconnect_backoff,
            health: self.health.clone(),
            watchers: Mutex::new(BTreeMap::new()),
        });
        connection
            .health
            .publish(ConnectorState::Connecting, 0, None)
            .await;
        Ok(connection)
    }
}

#[async_trait]
impl PortfolioConnector for BinancePortfolioConnector {
    async fn connect(&self, cex_id: CexId) -> BpResult<Arc<dyn PortfolioConnection>> {
        self.connect_with_sink_optional(cex_id, None).await
    }

    async fn connect_with_sink(
        &self,
        cex_id: CexId,
        sink: Arc<dyn PortfolioUpdateSink>,
    ) -> BpResult<Arc<dyn PortfolioConnection>> {
        self.connect_with_sink_optional(cex_id, Some(sink)).await
    }
}

struct Watcher {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

struct BinancePortfolioConnection {
    transport: Arc<dyn BinanceTransport>,
    credentials: Arc<dyn BinanceCredentialSource>,
    sink: Option<Arc<dyn PortfolioUpdateSink>>,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
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
                account: AccountKey::new(CexId::Binance, &account.0),
                transport: Arc::clone(&self.transport),
                credentials: Arc::clone(&self.credentials),
                sink: self.sink.clone(),
                listen_key_refresh: self.listen_key_refresh,
                reconnect_backoff: self.reconnect_backoff,
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
