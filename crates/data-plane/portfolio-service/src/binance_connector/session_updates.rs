use super::session::WatcherConfig;
use crate::{
    AccountPortfolio, PortfolioUpdate, feed::PortfolioUpdateApply, health::ConnectorState,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use platform_metrics::PortfolioMetrics;
use std::{collections::BTreeMap, time::Instant};
use stock_trek_types::cex::asset_id::AssetId;

const CEX: &str = "binance";

pub(super) struct SessionState {
    pub version: u64,
    pub balances: BTreeMap<AssetId, f64>,
    pub last_update: Option<Instant>,
    pub last_exchange_time: DateTime<Utc>,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            version: 0,
            balances: BTreeMap::new(),
            last_update: None,
            last_exchange_time: Utc::now(),
        }
    }

    pub fn replace_snapshot(
        &mut self,
        balances: BTreeMap<AssetId, f64>,
        occurred_at: DateTime<Utc>,
    ) {
        self.balances = balances;
        self.last_exchange_time = occurred_at;
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl WatcherConfig {
    pub(super) fn next_version(current: u64, exchange_version: u64) -> u64 {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        current.max(exchange_version).max(now).saturating_add(1)
    }

    pub(super) fn merge_balances(
        target: &mut BTreeMap<AssetId, f64>,
        updates: &BTreeMap<AssetId, f64>,
    ) {
        target.extend(updates.iter().map(|(asset, total)| (*asset, *total)));
    }

    pub(super) async fn publish_portfolio(
        &self,
        version: u64,
        balances: &BTreeMap<AssetId, f64>,
        occurred_at: DateTime<Utc>,
        exchange_time: Option<DateTime<Utc>>,
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

    pub(super) async fn publish_health(&self, state: ConnectorState, last_update: Option<Instant>) {
        self.health.publish(state, 1, last_update).await;
    }
}
