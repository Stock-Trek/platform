use super::limit_service::{
    LimitService, LimitSources, QUOTA_AD_HOC_RUNS_PER_HOUR, QUOTA_ALGORITHMS,
    QUOTA_API_REQUESTS_PER_MINUTE, QUOTA_BOTS, QUOTA_CONCURRENT_BACKTESTS, QUOTA_LINKED_ACCOUNTS,
    QUOTA_MAX_REQUEST_BYTES, QUOTA_MAX_RESULT_BYTES, QUOTA_SCHEDULED_RUNS_PER_DAY,
    QUOTA_STREAM_CONNECTIONS, QUOTA_STREAM_SUBSCRIPTIONS,
};
use crate::accounts::AccountStore;
use crate::algorithms::BuildStore;
use crate::backtests::BacktestStore;
use crate::bots::BotStore;
use crate::streaming::ChannelRegistry;
use chrono::Utc;
use std::sync::Arc;

fn service() -> LimitService {
    let registry = || Arc::new(ChannelRegistry::new());
    LimitService::new(LimitSources {
        accounts: AccountStore::new().into(),
        builds: BuildStore::new(registry()).into(),
        bots: BotStore::new().into(),
        backtests: BacktestStore::new(registry()).into(),
    })
}

const QUOTAS: [&str; 11] = [
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
];

#[test]
fn effective_limits_cover_every_tier_based_quota() {
    let effective = service().effective("user_7gk2", Utc::now());
    for quota in QUOTAS {
        let entry = effective
            .get(quota)
            .unwrap_or_else(|| panic!("{quota} is missing from the effective limits"));
        assert!(entry.limit > 0, "{quota} has no limit");
        assert!(
            entry.used <= entry.limit,
            "{quota} reports used above limit"
        );
    }
}

#[test]
fn limits_are_scoped_per_owner() {
    let service = service();
    let first = service.effective("user_7gk2", Utc::now());
    let second = service.effective("user_9zk", Utc::now());
    assert_eq!(
        first.get(QUOTA_MAX_REQUEST_BYTES).map(|entry| entry.limit),
        second.get(QUOTA_MAX_REQUEST_BYTES).map(|entry| entry.limit)
    );
    assert_eq!(
        first.get(QUOTA_MAX_REQUEST_BYTES).map(|entry| entry.used),
        Some(0)
    );
}

#[test]
fn a_replacement_frees_the_quota_it_replaces() {
    let bots = BotStore::new();
    for bot_id in ["bot_1", "bot_2"] {
        bots.insert(
            "user_7gk2",
            crate::bots::Bot {
                bot_id: bot_id.to_string(),
                name: bot_id.to_string(),
                algorithm_id: "algorithm_1".to_string(),
                build_id: "build_1".to_string(),
                strategy_name: "momentum_v2".to_string(),
                account_ids: vec!["account_1".to_string()],
                schedule: crate::bots::bot_types::Schedule::Interval {
                    every_seconds: 10,
                    misfire_grace_seconds: None,
                },
                state: crate::bots::BotState::Enabled,
                active_run_id: None,
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                stream_sequence: 0,
            },
        );
    }
    let registry = Arc::new(ChannelRegistry::new());
    let service = LimitService::new(LimitSources {
        accounts: AccountStore::new().into(),
        builds: BuildStore::new(registry.clone()).into(),
        bots: bots.into(),
        backtests: BacktestStore::new(registry).into(),
    });
    let now = Utc::now();
    service
        .check("user_7gk2", QUOTA_SCHEDULED_RUNS_PER_DAY, 10_000, now)
        .expect_err("usage above the limit fails");
    service
        .check_replacement(
            "user_7gk2",
            QUOTA_SCHEDULED_RUNS_PER_DAY,
            8_640,
            10_000,
            now,
        )
        .expect("replacing a bot's own contribution frees room");
}
