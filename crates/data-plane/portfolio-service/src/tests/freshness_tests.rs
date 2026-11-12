use crate::{FreshnessPolicy, PortfolioFreshness, StaleOrderPolicy};
use chrono::Duration;

#[test]
fn evaluates_missing_fresh_and_stale_updates() {
    let policy =
        FreshnessPolicy::new(Duration::seconds(30), StaleOrderPolicy::Reject).expect("policy");
    assert_eq!(policy.evaluate(None), PortfolioFreshness::Missing);
    assert_eq!(
        policy.evaluate(Some(Duration::seconds(30))),
        PortfolioFreshness::Fresh
    );
    assert_eq!(
        policy.evaluate(Some(Duration::seconds(31))),
        PortfolioFreshness::Stale
    );
}

#[test]
fn rejects_stale_orders_when_configured() {
    let policy =
        FreshnessPolicy::new(Duration::seconds(30), StaleOrderPolicy::Reject).expect("policy");
    assert!(policy.authorize_order(Some(Duration::seconds(31))).is_err());
    assert_eq!(
        policy
            .authorize_order(Some(Duration::seconds(10)))
            .expect("fresh"),
        PortfolioFreshness::Fresh
    );
}

#[test]
fn degraded_and_allowed_policies_return_staleness_without_rejecting() {
    for stale_policy in [StaleOrderPolicy::Allow, StaleOrderPolicy::Degrade] {
        let policy = FreshnessPolicy::new(Duration::seconds(30), stale_policy).expect("policy");
        assert_eq!(
            policy
                .authorize_order(Some(Duration::seconds(90)))
                .expect("stale allowed"),
            PortfolioFreshness::Stale
        );
    }
}

#[test]
fn parses_stale_order_policy_names() {
    assert_eq!(
        "allow".parse::<StaleOrderPolicy>().expect("allow"),
        StaleOrderPolicy::Allow
    );
    assert_eq!(
        "DEGRADE".parse::<StaleOrderPolicy>().expect("degrade"),
        StaleOrderPolicy::Degrade
    );
    assert_eq!(
        "reject".parse::<StaleOrderPolicy>().expect("reject"),
        StaleOrderPolicy::Reject
    );
    assert!("other".parse::<StaleOrderPolicy>().is_err());
}

#[tokio::test]
async fn order_authorization_uses_the_configured_staleness_policy() {
    let feed = std::sync::Arc::new(crate::tests::test_feed_tests::RecordingFeed::default());
    let account = crate::tests::test_feed_tests::Fixture::account("primary");
    let portfolio = crate::tests::test_feed_tests::Fixture::portfolio(
        stock_trek_types::cex::asset_id::AssetId::Bitcoin,
        1.0,
    );
    let stale = crate::PortfolioUpdate::versioned(
        account.clone(),
        None,
        portfolio,
        1,
        chrono::Utc::now() - Duration::seconds(120),
    );

    let reject = crate::PortfolioService::with_freshness_policy(
        feed.clone(),
        FreshnessPolicy::new(Duration::seconds(30), StaleOrderPolicy::Reject).expect("policy"),
    );
    reject
        .track_user_account("owner", account.clone())
        .await
        .expect("track");
    assert!(reject.apply_portfolio_update(stale.clone()) == crate::PortfolioUpdateOutcome::Applied);
    assert!(
        reject
            .authorize_order("owner", &account, chrono::Utc::now())
            .is_err()
    );

    let degrade = crate::PortfolioService::with_freshness_policy(
        feed,
        FreshnessPolicy::new(Duration::seconds(30), StaleOrderPolicy::Degrade).expect("policy"),
    );
    degrade
        .track_user_account("owner", account.clone())
        .await
        .expect("track");
    assert!(degrade.apply_portfolio_update(stale) == crate::PortfolioUpdateOutcome::Applied);
    assert_eq!(
        degrade
            .authorize_order("owner", &account, chrono::Utc::now())
            .expect("degraded"),
        PortfolioFreshness::Stale
    );
}
