use super::{
    credentials::BinanceCredentialSource,
    transport::{BinanceAccountEvent, BinanceTransport},
};
use crate::{
    AccountKey, AccountPortfolio, PortfolioUpdate,
    feed::{PortfolioConnection, PortfolioConnector, PortfolioUpdateApply, PortfolioUpdateSink},
    health::{CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorState},
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use platform_metrics::PortfolioMetrics;
use platform_nats::EventPublisher;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant as TokioInstant, interval_at, sleep, timeout},
};

const CEX: &str = "binance";
const HEALTH_INTERVAL: Duration = Duration::from_secs(30);

pub struct BinancePortfolioConnector {
    transport: Arc<dyn BinanceTransport>,
    credentials: Arc<dyn BinanceCredentialSource>,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
    health_publisher: Option<Arc<dyn EventPublisher>>,
    health_subject: String,
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
            health_publisher: None,
            health_subject: CONNECTOR_HEALTH_SUBJECT.to_string(),
        }
    }

    pub fn with_health_publisher(
        mut self,
        publisher: Arc<dyn EventPublisher>,
        subject: impl Into<String>,
    ) -> Self {
        self.health_publisher = Some(publisher);
        self.health_subject = subject.into();
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
            health_publisher: self.health_publisher.clone(),
            health_subject: self.health_subject.clone(),
            watchers: tokio::sync::Mutex::new(BTreeMap::new()),
        });
        connection
            .publish_health(ConnectorState::Connecting, 0)
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
    health_publisher: Option<Arc<dyn EventPublisher>>,
    health_subject: String,
    watchers: tokio::sync::Mutex<BTreeMap<AccountId, Watcher>>,
}

impl BinancePortfolioConnection {
    async fn publish_health(&self, state: ConnectorState, subscriptions: usize) {
        let Some(publisher) = &self.health_publisher else {
            return;
        };
        let health = ConnectorHealth::new(
            CexId::Binance,
            state,
            subscriptions as u64,
            None,
            Utc::now(),
        );
        if let Ok(payload) = health.to_bytes()
            && let Err(error) = publisher.publish(&self.health_subject, &payload).await
        {
            tracing::warn!(%error, "failed to publish portfolio connector health");
        }
    }

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
                health_publisher: self.health_publisher.clone(),
                health_subject: self.health_subject.clone(),
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
        self.publish_health(ConnectorState::Connected, count).await;
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
        self.publish_health(ConnectorState::Disconnected, 0).await;
        Ok(())
    }
}

struct WatcherConfig {
    account: AccountKey,
    transport: Arc<dyn BinanceTransport>,
    credentials: Arc<dyn BinanceCredentialSource>,
    sink: Option<Arc<dyn PortfolioUpdateSink>>,
    listen_key_refresh: Duration,
    reconnect_backoff: Duration,
    health_publisher: Option<Arc<dyn EventPublisher>>,
    health_subject: String,
    shutdown: watch::Receiver<bool>,
}

impl WatcherConfig {
    async fn run(self) {
        let mut version = 0u64;
        let mut balances = BTreeMap::<AssetId, f64>::new();
        let mut last_update = None;
        let mut shutdown = self.shutdown.clone();
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self
                .run_session(&mut shutdown, &mut version, &mut balances, &mut last_update)
                .await
            {
                Ok(()) => return,
                Err(error) => {
                    PortfolioMetrics::record_connection_state(CEX, false);
                    PortfolioMetrics::record_reconnect(CEX);
                    tracing::warn!(account = %self.account.account_id().0, %error, "portfolio feed session failed");
                    self.publish_health(ConnectorState::Disconnected, last_update)
                        .await;
                    tokio::select! {
                        _ = sleep(self.reconnect_backoff) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn run_session(
        &self,
        shutdown: &mut watch::Receiver<bool>,
        version: &mut u64,
        balances: &mut BTreeMap<AssetId, f64>,
        last_update: &mut Option<Instant>,
    ) -> BpResult<()> {
        let credentials = self.credentials.credentials(&self.account).await?;
        let listen_key = self.transport.create_listen_key(&credentials).await?;
        let mut stream = self.transport.open_stream(&listen_key).await?;
        let snapshot = self.transport.account_snapshot(&credentials).await?;
        *balances = snapshot.balances().clone();
        *version = Self::next_version(*version, 0);
        self.publish_portfolio(
            *version,
            balances,
            snapshot.occurred_at(),
            None,
            last_update,
        )
        .await?;
        PortfolioMetrics::record_connection_state(CEX, true);
        self.publish_health(ConnectorState::Connected, *last_update)
            .await;
        let mut keepalive = interval_at(
            TokioInstant::now() + self.listen_key_refresh,
            self.listen_key_refresh,
        );
        let mut heartbeat = interval_at(TokioInstant::now() + HEALTH_INTERVAL, HEALTH_INTERVAL);
        let mut last_exchange_time = snapshot.occurred_at();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = self.transport.close_listen_key(&credentials, &listen_key).await;
                        return Ok(());
                    }
                }
                _ = keepalive.tick() => {
                    self.transport.keepalive_listen_key(&credentials, &listen_key).await?;
                }
                _ = heartbeat.tick() => {
                    self.publish_health(ConnectorState::Connected, *last_update).await;
                }
                event = stream.next() => {
                    match event? {
                        None => return Err(BpError::external(std::io::Error::other("Binance stream closed"))),
                        Some(BinanceAccountEvent::ListenKeyExpired) => {
                            return Err(BpError::external(std::io::Error::other("Binance listen key expired")));
                        }
                        Some(BinanceAccountEvent::Position(event)) => {
                            if event.occurred_at() < last_exchange_time {
                                continue;
                            }
                            last_exchange_time = event.occurred_at();
                            Self::merge_balances(balances, event.balances());
                            *version = Self::next_version(*version, event.version());
                            *last_update = Some(Instant::now());
                            self.publish_portfolio(
                                *version,
                                balances,
                                Utc::now(),
                                Some(event.occurred_at()),
                                last_update,
                            )
                            .await?;
                            self.publish_health(ConnectorState::Connected, *last_update)
                                .await;
                        }
                    }
                }
            }
        }
    }

    fn next_version(current: u64, exchange_version: u64) -> u64 {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        current.max(exchange_version).max(now).saturating_add(1)
    }

    fn merge_balances(target: &mut BTreeMap<AssetId, f64>, updates: &BTreeMap<AssetId, f64>) {
        target.extend(updates.iter().map(|(asset, total)| (*asset, *total)));
    }

    async fn publish_portfolio(
        &self,
        version: u64,
        balances: &BTreeMap<AssetId, f64>,
        occurred_at: chrono::DateTime<Utc>,
        exchange_time: Option<chrono::DateTime<Utc>>,
        last_update: &mut Option<Instant>,
    ) -> BpResult<()> {
        let Some(sink) = &self.sink else {
            return Ok(());
        };
        let portfolio =
            AccountPortfolio::new(balances.clone(), Vec::new()).map_err(BpError::external)?;
        let mut update =
            PortfolioUpdate::versioned(self.account.clone(), None, portfolio, version, occurred_at);
        if let Some(exchange_time) = exchange_time {
            update = update.with_exchange_time(exchange_time);
        }
        match sink.apply_update(update).await? {
            PortfolioUpdateApply::Applied => {
                PortfolioMetrics::record_update_outcome(CEX, "applied");
            }
            PortfolioUpdateApply::IgnoredUntracked => {
                PortfolioMetrics::record_update_outcome(CEX, "untracked");
            }
            PortfolioUpdateApply::IgnoredStale => {
                PortfolioMetrics::record_update_outcome(CEX, "stale");
            }
        }
        *last_update = Some(Instant::now());
        Ok(())
    }

    async fn publish_health(&self, state: ConnectorState, last_update: Option<Instant>) {
        if let Some(last_update) = last_update {
            PortfolioMetrics::record_connector_update_age(CEX, last_update.elapsed());
        }
        let Some(publisher) = &self.health_publisher else {
            return;
        };
        let health = ConnectorHealth::new(
            CexId::Binance,
            state,
            1,
            last_update.map(|instant| instant.elapsed().as_secs()),
            Utc::now(),
        );
        if let Ok(payload) = health.to_bytes()
            && let Err(error) = publisher.publish(&self.health_subject, &payload).await
        {
            tracing::warn!(%error, "failed to publish portfolio connector health");
        }
    }
}
