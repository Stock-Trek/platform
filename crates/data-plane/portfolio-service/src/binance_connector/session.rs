use super::{
    credentials::BinanceCredentialSource,
    health_reporter::HealthReporter,
    session_updates::SessionState,
    transport::{BinanceAccountEvent, BinanceTransport},
};
use crate::{AccountKey, PortfolioUpdateSink, health::ConnectorState};
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use platform_metrics::PortfolioMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::watch,
    time::{Instant as TokioInstant, interval_at, sleep},
};

const CEX: &str = "binance";
const HEALTH_INTERVAL: Duration = Duration::from_secs(30);

pub(super) struct WatcherConfig {
    pub(super) account: AccountKey,
    pub(super) transport: Arc<dyn BinanceTransport>,
    pub(super) credentials: Arc<dyn BinanceCredentialSource>,
    pub(super) sink: Option<Arc<dyn PortfolioUpdateSink>>,
    pub(super) listen_key_refresh: Duration,
    pub(super) reconnect_backoff: Duration,
    pub(super) health: HealthReporter,
    pub(super) shutdown: watch::Receiver<bool>,
}

impl WatcherConfig {
    pub async fn run(self) {
        let mut state = SessionState::new();
        let mut shutdown = self.shutdown.clone();
        loop {
            if *shutdown.borrow() {
                return;
            }
            match self.run_session(&mut shutdown, &mut state).await {
                Ok(()) => return,
                Err(error) => {
                    PortfolioMetrics::record_connection_state(CEX, false);
                    PortfolioMetrics::record_reconnect(CEX);
                    tracing::warn!(
                        account = %self.account.account_id().0,
                        %error,
                        "portfolio feed session failed"
                    );
                    self.publish_health(ConnectorState::Disconnected, state.last_update)
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
        state: &mut SessionState,
    ) -> BpResult<()> {
        let credentials = self.credentials.credentials(&self.account).await?;
        let listen_key = self.transport.create_listen_key(&credentials).await?;
        let result = self
            .session(&credentials, &listen_key, shutdown, state)
            .await;
        let _ = self
            .transport
            .close_listen_key(&credentials, &listen_key)
            .await;
        result
    }

    async fn session(
        &self,
        credentials: &platform_exchange_types::api_key_credential::ApiKeyCredentials,
        listen_key: &str,
        shutdown: &mut watch::Receiver<bool>,
        state: &mut SessionState,
    ) -> BpResult<()> {
        let mut stream = self.transport.open_stream(listen_key).await?;
        let snapshot = self.transport.account_snapshot(credentials).await?;
        state.replace_snapshot(snapshot.balances().clone(), snapshot.occurred_at());
        state.version = Self::next_version(state.version, 0);
        self.publish_portfolio(
            state.version,
            &state.balances,
            snapshot.occurred_at(),
            None,
            &mut state.last_update,
        )
        .await?;
        PortfolioMetrics::record_connection_state(CEX, true);
        self.publish_health(ConnectorState::Connected, state.last_update)
            .await;
        let mut keepalive = interval_at(
            TokioInstant::now() + self.listen_key_refresh,
            self.listen_key_refresh,
        );
        let mut heartbeat = interval_at(TokioInstant::now() + HEALTH_INTERVAL, HEALTH_INTERVAL);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = keepalive.tick() => {
                    self.transport.keepalive_listen_key(credentials, listen_key).await?;
                }
                _ = heartbeat.tick() => {
                    self.publish_health(ConnectorState::Connected, state.last_update).await;
                }
                event = stream.next() => {
                    match event? {
                        None => return Err(BpError::external(std::io::Error::other("Binance stream closed"))),
                        Some(BinanceAccountEvent::ListenKeyExpired) => {
                            return Err(BpError::external(std::io::Error::other("Binance listen key expired")));
                        }
                        Some(BinanceAccountEvent::Position(event)) => {
                            if event.occurred_at() < state.last_exchange_time {
                                continue;
                            }
                            state.last_exchange_time = event.occurred_at();
                            Self::merge_balances(&mut state.balances, event.balances());
                            state.version = Self::next_version(state.version, event.version());
                            state.last_update = Some(Instant::now());
                            self.publish_portfolio(
                                state.version,
                                &state.balances,
                                Utc::now(),
                                Some(event.occurred_at()),
                                &mut state.last_update,
                            ).await?;
                            self.publish_health(ConnectorState::Connected, state.last_update).await;
                        }
                    }
                }
            }
        }
    }
}
