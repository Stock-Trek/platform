use super::test_feed_tests::Fixture;
use crate::AccountKey;
use std::collections::BTreeSet;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[tokio::test]
async fn tracking_accounts_groups_them_per_exchange() {
    let (feed, service) = Fixture::service();
    assert!(
        service
            .track_account(Fixture::account("first"))
            .await
            .expect("track")
    );
    assert!(
        service
            .track_account(Fixture::account("second"))
            .await
            .expect("track")
    );

    let calls = feed.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].cex_id, CexId::Binance);
    assert_eq!(calls[0].accounts, BTreeSet::from([AccountId::new("first")]));
    assert_eq!(
        calls[1].accounts,
        BTreeSet::from([AccountId::new("first"), AccountId::new("second")])
    );
}

#[tokio::test]
async fn duplicate_tracking_does_not_notify_the_feed() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("first");
    assert!(service.track_account(account.clone()).await.expect("track"));
    assert!(!service.track_account(account).await.expect("track"));
    assert_eq!(feed.calls().len(), 1);
}

#[tokio::test]
async fn untracking_updates_the_feed_subscription() {
    let (feed, service) = Fixture::service();
    service
        .track_account(Fixture::account("first"))
        .await
        .expect("track");
    service
        .track_account(Fixture::account("second"))
        .await
        .expect("track");
    assert!(
        service
            .untrack_account(Fixture::account("first"))
            .await
            .expect("untrack")
    );

    let calls = feed.calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[2].accounts,
        BTreeSet::from([AccountId::new("second")])
    );
    assert_eq!(service.account_count(), 1);
}

#[tokio::test]
async fn feed_failures_do_not_change_tracked_accounts() {
    let (feed, service) = Fixture::service();
    feed.fail_next();
    assert!(
        service
            .track_account(Fixture::account("first"))
            .await
            .is_err()
    );
    assert!(service.tracked_accounts().is_empty());
    assert!(feed.calls().is_empty());
}

#[tokio::test]
async fn feed_failures_do_not_change_owner_tracking() {
    let (feed, service) = Fixture::service();
    feed.fail_next();
    assert!(
        service
            .track_user_account("alice", Fixture::account("first"))
            .await
            .is_err()
    );
    assert!(service.tracked_user_accounts("alice").is_empty());
}

#[tokio::test]
async fn owners_can_share_an_account_without_reconnecting() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("shared");
    service
        .track_user_account("alice", account.clone())
        .await
        .expect("track");
    service
        .track_user_account("bob", account.clone())
        .await
        .expect("track");
    assert_eq!(feed.calls().len(), 1);
    assert_eq!(
        service.tracked_user_accounts("alice"),
        vec![account.clone()]
    );
    assert_eq!(service.tracked_user_accounts("bob"), vec![account]);

    service
        .untrack_user_account("alice", Fixture::account("shared"))
        .await
        .expect("untrack");
    assert_eq!(feed.calls().len(), 1);
    assert_eq!(service.account_count(), 1);
}

#[tokio::test]
async fn owner_portfolios_are_scoped_to_the_owner() {
    let (_feed, service) = Fixture::service();
    let alice = Fixture::account("alice-account");
    let bob = Fixture::account("bob-account");
    service
        .track_user_account("alice", alice.clone())
        .await
        .expect("track");
    service
        .track_user_account("bob", bob.clone())
        .await
        .expect("track");
    service.apply_update(Fixture::update("alice-account", 1.0));
    service.apply_update(Fixture::update("bob-account", 2.0));

    assert!(service.user_account_portfolio("alice", &alice).is_some());
    assert!(service.user_account_portfolio("alice", &bob).is_none());
    assert_eq!(service.user_portfolios("alice").len(), 1);
    assert_eq!(service.portfolios().len(), 1);
}

#[tokio::test]
async fn updates_for_untracked_accounts_are_ignored() {
    let (_feed, service) = Fixture::service();
    assert!(!service.apply_update(Fixture::update("unknown", 1.0)));
    assert!(service.portfolios().is_empty());
}

#[tokio::test]
async fn account_aliases_delegate_to_tracking() {
    let (_feed, service) = Fixture::service();
    let account = Fixture::account("first");
    assert!(service.add_account(account.clone()).await.expect("add"));
    assert!(service.account_portfolio(&account).is_none());
    assert!(service.remove_account(account).await.expect("remove"));
    assert_eq!(service.account_count(), 0);
}

#[tokio::test]
async fn account_portfolio_returns_the_latest_update() {
    let (_feed, service) = Fixture::service();
    let account = Fixture::account("first");
    service.track_account(account.clone()).await.expect("track");
    service.apply_update(Fixture::update("first", 3.0));
    let portfolio = service.account_portfolio(&account).expect("portfolio");
    assert_eq!(
        portfolio.asset_total(stock_trek_types::cex::asset_id::AssetId::Bitcoin),
        3.0
    );
}

#[test]
fn account_key_is_ordered_and_hashable() {
    let first = Fixture::account("first");
    let second = AccountKey::new(CexId::Binance, "second");
    assert!(first < second);
    let mut values = BTreeSet::new();
    values.insert(first);
    assert_eq!(values.len(), 1);
}

#[tokio::test]
async fn remove_account_removes_owner_references_too() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("shared");
    service
        .track_user_account("alice", account.clone())
        .await
        .expect("track");
    assert!(service.remove_account(account).await.expect("remove"));
    assert!(service.tracked_user_accounts("alice").is_empty());
    assert_eq!(service.account_count(), 0);
    assert!(feed.calls().last().expect("call").accounts.is_empty());
}
