use crate::accounts::AccountStore;
use crate::algorithms::BuildStore;
use crate::api::{MAX_REQUEST_BODY_BYTES, Problem, QuotaDetail};
use crate::backtests::BacktestStore;
use crate::bots::{BotState, BotStore};
use crate::limits::throttle::Throttle;
use crate::streaming::MAX_SUBSCRIPTIONS_PER_CONNECTION;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

pub const QUOTA_LINKED_ACCOUNTS: &str = "linkedAccounts";
pub const QUOTA_ALGORITHMS: &str = "algorithms";
pub const QUOTA_BOTS: &str = "bots";
pub const QUOTA_SCHEDULED_RUNS_PER_DAY: &str = "scheduledRunsPerDay";
pub const QUOTA_CONCURRENT_BACKTESTS: &str = "concurrentBacktests";
pub const QUOTA_STREAM_CONNECTIONS: &str = "streamConnections";
pub const QUOTA_STREAM_SUBSCRIPTIONS: &str = "concurrentStreamSubscriptions";
pub const QUOTA_API_REQUESTS_PER_MINUTE: &str = "apiRequestsPerMinute";
pub const QUOTA_AD_HOC_RUNS_PER_HOUR: &str = "adHocRunsPerHour";
pub const QUOTA_MAX_REQUEST_BYTES: &str = "maxRequestBytes";
pub const QUOTA_MAX_RESULT_BYTES: &str = "maxBacktestResultBytes";

const REQUEST_RATE_WINDOW: Duration = Duration::minutes(1);
const AD_HOC_RATE_WINDOW: Duration = Duration::hours(1);
pub const MAX_RESULT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitEntry {
    pub limit: u64,
    pub used: u64,
}

pub struct LimitSources {
    pub accounts: Arc<AccountStore>,
    pub builds: Arc<BuildStore>,
    pub bots: Arc<BotStore>,
    pub backtests: Arc<BacktestStore>,
}

pub struct LimitService {
    sources: LimitSources,
    request_throttle: Throttle,
    ad_hoc_throttle: Throttle,
}

impl LimitService {
    pub fn new(sources: LimitSources) -> Self {
        Self {
            sources,
            request_throttle: Throttle::new(REQUEST_RATE_WINDOW),
            ad_hoc_throttle: Throttle::new(AD_HOC_RATE_WINDOW),
        }
    }

    pub fn effective(&self, owner: &str, now: DateTime<Utc>) -> BTreeMap<&'static str, LimitEntry> {
        let mut entries = BTreeMap::new();
        for quota in [
            QUOTA_LINKED_ACCOUNTS,
            QUOTA_ALGORITHMS,
            QUOTA_BOTS,
            QUOTA_SCHEDULED_RUNS_PER_DAY,
            QUOTA_CONCURRENT_BACKTESTS,
            QUOTA_STREAM_CONNECTIONS,
            QUOTA_STREAM_SUBSCRIPTIONS,
            QUOTA_API_REQUESTS_PER_MINUTE,
            QUOTA_AD_HOC_RUNS_PER_HOUR,
            QUOTA_MAX_REQUEST_BYTES,
            QUOTA_MAX_RESULT_BYTES,
        ] {
            entries.insert(quota, self.entry(owner, quota, now));
        }
        entries
    }

    pub fn check(
        &self,
        owner: &str,
        quota: &'static str,
        requested: u64,
        now: DateTime<Utc>,
    ) -> Result<(), Problem> {
        let entry = self.entry(owner, quota, now);
        if entry.used + requested <= entry.limit {
            Ok(())
        } else {
            Err(Problem::quota_exceeded(QuotaDetail {
                quota,
                limit: entry.limit,
                used: entry.used,
            }))
        }
    }

    pub fn check_replacement(
        &self,
        owner: &str,
        quota: &'static str,
        current: u64,
        requested: u64,
        now: DateTime<Utc>,
    ) -> Result<(), Problem> {
        let entry = self.entry(owner, quota, now);
        if entry.used.saturating_sub(current) + requested <= entry.limit {
            Ok(())
        } else {
            Err(Problem::quota_exceeded(QuotaDetail {
                quota,
                limit: entry.limit,
                used: entry.used,
            }))
        }
    }

    pub fn check_rate(
        &self,
        owner: &str,
        quota: &'static str,
        requested: u64,
        retry_after: u64,
        now: DateTime<Utc>,
    ) -> Result<(), Problem> {
        let entry = self.entry(owner, quota, now);
        if entry.used + requested <= entry.limit {
            Ok(())
        } else {
            Err(Problem::rate_limited(retry_after))
        }
    }

    pub fn check_request_rate(&self, owner: &str, now: DateTime<Utc>) -> Result<(), Problem> {
        let limit = self.limit(QUOTA_API_REQUESTS_PER_MINUTE);
        self.request_throttle
            .check(owner, limit, now)
            .map_err(Problem::rate_limited)
    }

    pub fn check_ad_hoc_run_rate(&self, bot_id: &str, now: DateTime<Utc>) -> Result<(), Problem> {
        let limit = self.limit(QUOTA_AD_HOC_RUNS_PER_HOUR);
        self.ad_hoc_throttle
            .check(bot_id, limit, now)
            .map_err(Problem::rate_limited)
    }

    pub fn entry(&self, owner: &str, quota: &str, now: DateTime<Utc>) -> LimitEntry {
        LimitEntry {
            limit: self.limit(quota),
            used: self.used(owner, quota, now),
        }
    }

    fn limit(&self, quota: &str) -> u64 {
        match quota {
            QUOTA_LINKED_ACCOUNTS => 20,
            QUOTA_ALGORITHMS => 100,
            QUOTA_BOTS => 50,
            QUOTA_SCHEDULED_RUNS_PER_DAY => 20_000,
            QUOTA_CONCURRENT_BACKTESTS => 2,
            QUOTA_STREAM_CONNECTIONS => 5,
            QUOTA_STREAM_SUBSCRIPTIONS => MAX_SUBSCRIPTIONS_PER_CONNECTION as u64,
            QUOTA_API_REQUESTS_PER_MINUTE => 600,
            QUOTA_AD_HOC_RUNS_PER_HOUR => 60,
            QUOTA_MAX_REQUEST_BYTES => MAX_REQUEST_BODY_BYTES as u64,
            QUOTA_MAX_RESULT_BYTES => MAX_RESULT_BYTES,
            _ => 0,
        }
    }

    fn used(&self, owner: &str, quota: &str, now: DateTime<Utc>) -> u64 {
        match quota {
            QUOTA_LINKED_ACCOUNTS => self.sources.accounts.count(owner) as u64,
            QUOTA_ALGORITHMS => self.sources.builds.list_algorithms(owner).len() as u64,
            QUOTA_BOTS => self.sources.bots.count(owner) as u64,
            QUOTA_SCHEDULED_RUNS_PER_DAY => self
                .sources
                .bots
                .all(owner)
                .iter()
                .filter(|bot| bot.state == BotState::Enabled)
                .map(|bot| bot.schedule.runs_per_day())
                .sum(),
            QUOTA_CONCURRENT_BACKTESTS => self.sources.backtests.count_active(owner) as u64,
            QUOTA_STREAM_CONNECTIONS | QUOTA_STREAM_SUBSCRIPTIONS => 0,
            QUOTA_AD_HOC_RUNS_PER_HOUR => self
                .sources
                .bots
                .all(owner)
                .iter()
                .map(|bot| self.ad_hoc_throttle.used(&bot.bot_id, now))
                .sum(),
            QUOTA_MAX_REQUEST_BYTES | QUOTA_MAX_RESULT_BYTES => 0,
            QUOTA_API_REQUESTS_PER_MINUTE => self.request_throttle.used(owner, now),
            _ => 0,
        }
    }
}
