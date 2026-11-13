use crate::connector::portfolio::{
    ConnectorPortfolioUpdate, ConnectorState, PortfolioUpdateApply, PortfolioUpdateSink,
    health_reporter::HealthReporter, session_state::SessionState,
};
use crate::connector::portfolio::{PortfolioSession, PortfolioTransport};
use chrono::Utc;
use platform_errors::{BuildError, BuildResult};
use platform_exchange_types::portfolio::{AccountEvent, AccountPositionEvent, Credential};
use platform_metrics::PortfolioMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};
use tokio::{
    sync::watch,
    time::{Instant as TokioInstant, interval_at, sleep},
};

const HEALTH_INTERVAL: Duration = Duration::from_secs(30);

pub(super) struct Watcher {
    pub(super) cex_id: CexId,
    pub(super) account: AccountId,
    pub(super) transport: Arc<dyn PortfolioTransport>,
    pub(super) credentials: Arc<dyn crate::connector::portfolio::PortfolioCredentialSource>,
    pub(super) sink: Arc<dyn PortfolioUpdateSink>,
    pub(super) health: HealthReporter,
    pub(super) shutdown: watch::Receiver<bool>,
}

impl Watcher {
    pub(super) async fn run(self) {
        let mut state = SessionState::new();
        let mut shutdown = self.shutdown.clone();
        let mut credential = None;
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self
                .run_session(&mut shutdown, &mut state, &mut credential)
                .await
            {
                Ok(()) => return,
                Err(error) => {
                    PortfolioMetrics::record_connection_state(self.cex_id.to_string(), false);
                    PortfolioMetrics::record_reconnect(self.cex_id.to_string());
                    tracing::warn!(
                        cex = %self.cex_id,
                        account = %self.account.0,
                        %error,
                        "portfolio feed session failed"
                    );
                    self.publish_health(ConnectorState::Disconnected, state.last_update)
                        .await;
                    tokio::select! {
                        _ = sleep(self.transport.reconnect_backoff()) => {}
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
        state: &mut SessionState,
        previous: &mut Option<Credential>,
    ) -> BuildResult<()> {
        let credentials = self
            .credentials
            .credentials(self.cex_id, &self.account)
            .await?;
        let mut session = PortfolioSession::open_with_credential(
            Arc::clone(&self.transport),
            &self.account,
            &credentials,
            previous.take(),
        )
        .await?;
        let result = self.session(&mut session, shutdown, state).await;
        if result.is_ok() {
            let _ = session.close().await;
            *previous = None;
            return result;
        }
        if session.reconnect_reuses_credential() {
            *previous = session
                .credential()
                .filter(|credential| !credential.is_expired(Instant::now()))
                .cloned();
        }
        result
    }

    async fn session(
        &self,
        session: &mut PortfolioSession,
        shutdown: &mut watch::Receiver<bool>,
        state: &mut SessionState,
    ) -> BuildResult<()> {
        let snapshot = session.snapshot().await?;
        state.replace_snapshot(snapshot.balances().clone(), snapshot.occurred_at());
        state.version = SessionState::next_version(state.version, 0);
        self.publish_portfolio(
            state.version,
            &state.balances,
            snapshot.occurred_at(),
            None,
            &mut state.last_update,
        )
        .await?;
        PortfolioMetrics::record_connection_state(self.cex_id.to_string(), true);
        self.publish_health(ConnectorState::Connected, state.last_update)
            .await;
        let refresh_interval = session.renewal_schedule().map(|schedule| {
            schedule
                .interval
                .saturating_sub(schedule.lead_time)
                .max(Duration::from_millis(1))
        });
        let mut refresh =
            refresh_interval.map(|interval| interval_at(TokioInstant::now() + interval, interval));
        let lifetime_deadline = session
            .max_session_lifetime()
            .map(|lifetime| TokioInstant::now() + lifetime);
        let mut heartbeat = interval_at(TokioInstant::now() + HEALTH_INTERVAL, HEALTH_INTERVAL);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = async {
                    match refresh.as_mut() {
                        Some(refresh) => { refresh.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    session.refresh().await?;
                }
                _ = async {
                    match lifetime_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    return Err(BuildError::external(std::io::Error::other("portfolio session lifetime expired")));
                }
                _ = heartbeat.tick() => {
                    self.publish_health(ConnectorState::Connected, state.last_update).await;
                }
                event = session.next() => {
                    match event? {
                        None => return Err(BuildError::external(std::io::Error::other("portfolio event stream closed"))),
                        Some(AccountEvent::SessionExpired) => {
                            return Err(BuildError::external(std::io::Error::other("portfolio session expired")));
                        }
                        Some(AccountEvent::Position(event)) => {
                            self.apply_position(event, state).await?;
                        }
                    }
                }
            }
        }
    }

    async fn apply_position(
        &self,
        event: AccountPositionEvent,
        state: &mut SessionState,
    ) -> BuildResult<()> {
        if event.occurred_at() < state.last_exchange_time {
            return Ok(());
        }
        state.last_exchange_time = event.occurred_at();
        SessionState::merge_balances(&mut state.balances, event.balances());
        state.version = SessionState::next_version(state.version, event.version());
        self.publish_portfolio(
            state.version,
            &state.balances,
            Utc::now(),
            Some(event.occurred_at()),
            &mut state.last_update,
        )
        .await?;
        self.publish_health(ConnectorState::Connected, state.last_update)
            .await;
        Ok(())
    }

    async fn publish_portfolio(
        &self,
        version: u64,
        balances: &std::collections::BTreeMap<stock_trek_types::cex::asset_id::AssetId, f64>,
        occurred_at: chrono::DateTime<Utc>,
        exchange_time: Option<chrono::DateTime<Utc>>,
        last_update: &mut Option<Instant>,
    ) -> BuildResult<()> {
        let update = ConnectorPortfolioUpdate::new(
            self.cex_id,
            self.account.clone(),
            balances.clone(),
            version,
            occurred_at,
        )
        .with_optional_exchange_time(exchange_time);
        match self.sink.apply_update(update).await? {
            PortfolioUpdateApply::Applied => {
                PortfolioMetrics::record_update_outcome(self.cex_id.to_string(), "applied");
            }
            PortfolioUpdateApply::IgnoredUntracked => {
                PortfolioMetrics::record_update_outcome(self.cex_id.to_string(), "untracked");
            }
            PortfolioUpdateApply::IgnoredStale => {
                PortfolioMetrics::record_update_outcome(self.cex_id.to_string(), "stale");
            }
        }
        *last_update = Some(Instant::now());
        Ok(())
    }

    async fn publish_health(&self, state: ConnectorState, last_update: Option<Instant>) {
        self.health.publish(state, 1, last_update).await;
    }
}
