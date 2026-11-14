use crate::connector::portfolio::{
    ConnectorState, PortfolioConnection, PortfolioCredentialSource, PortfolioExchange,
    PortfolioUpdateSink, account_watcher::AccountWatcher, health_reporter::HealthReporter,
    watcher::Watcher,
};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_metrics::PortfolioMetrics;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};
use tokio::sync::{Mutex, watch};

pub struct ExchangePortfolioConnection {
    cex_id: CexId,
    exchange: Arc<dyn PortfolioExchange>,
    credentials: Arc<dyn PortfolioCredentialSource>,
    sink: Arc<dyn PortfolioUpdateSink>,
    health: HealthReporter,
    watchers: Mutex<BTreeMap<AccountId, AccountWatcher>>,
}

impl ExchangePortfolioConnection {
    pub fn new(
        cex_id: CexId,
        exchange: Arc<dyn PortfolioExchange>,
        credentials: Arc<dyn PortfolioCredentialSource>,
        sink: Arc<dyn PortfolioUpdateSink>,
        health: HealthReporter,
    ) -> Self {
        Self {
            cex_id,
            exchange,
            credentials,
            sink,
            health,
            watchers: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn connecting(&self) {
        self.health
            .publish(ConnectorState::Connecting, 0, None)
            .await;
    }

    fn spawn(&self, account: AccountId) -> AccountWatcher {
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(
            Watcher {
                cex_id: self.cex_id,
                account,
                exchange: Arc::clone(&self.exchange),
                credentials: Arc::clone(&self.credentials),
                sink: Arc::clone(&self.sink),
                health: self.health.clone(),
                shutdown: receiver,
            }
            .run(),
        );
        AccountWatcher::new(shutdown, task)
    }
}

#[async_trait]
impl PortfolioConnection for ExchangePortfolioConnection {
    async fn update_accounts(&self, accounts: BTreeSet<AccountId>) -> PlatformResult<()> {
        let mut watchers = self.watchers.lock().await;
        let removed: Vec<AccountId> = watchers
            .keys()
            .filter(|account| !accounts.contains(*account))
            .cloned()
            .collect();
        for account in removed {
            if let Some(watcher) = watchers.remove(&account) {
                watcher.stop().await;
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

    async fn disconnect(&self) -> PlatformResult<()> {
        let watchers = {
            let mut watchers = self.watchers.lock().await;
            std::mem::take(&mut *watchers)
        };
        for watcher in watchers.into_values() {
            watcher.stop().await;
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
