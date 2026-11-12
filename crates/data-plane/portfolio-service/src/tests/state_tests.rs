use crate::tests::test_feed_tests::Fixture;
use crate::{AccountKey, PortfolioState};
use stock_trek_types::cex::asset_id::AssetId;

#[test]
fn state_tracks_global_and_owner_accounts_once() {
    let mut state = PortfolioState::new();
    let account = Fixture::account("primary");
    assert!(state.track(&account));
    assert!(!state.track(&account));
    assert!(state.track_for_owner("alice", &account));
    assert!(!state.track_for_owner("alice", &account));
    assert_eq!(state.tracked_accounts(), vec![account.clone()]);
    assert_eq!(state.tracked_accounts_for_owner("alice"), vec![account]);
}

#[test]
fn state_keeps_an_account_while_an_owner_still_tracks_it() {
    let mut state = PortfolioState::new();
    let account = Fixture::account("primary");
    state.track(&account);
    state.track_for_owner("alice", &account);
    state.track_for_owner("bob", &account);

    assert!(state.untrack(&account));
    assert!(state.is_tracked(&account));

    assert!(state.untrack_for_owner("alice", &account));
    assert!(state.is_tracked(&account));

    assert!(state.untrack_for_owner("bob", &account));
    assert!(!state.is_tracked(&account));
}

#[test]
fn state_applies_updates_only_for_tracked_accounts() {
    let mut state = PortfolioState::new();
    let account = Fixture::account("primary");
    assert!(!state.apply_update(Fixture::update("primary", 2.0)));

    state.track(&account);
    assert!(state.apply_update(Fixture::update("primary", 2.0)));
    let portfolio = state.portfolio(&account).expect("portfolio");
    assert_eq!(portfolio.asset_total(AssetId::Bitcoin), 2.0);
}

#[test]
fn state_removes_an_untracked_portfolio() {
    let mut state = PortfolioState::new();
    let account = Fixture::account("primary");
    state.track(&account);
    state.apply_update(Fixture::update("primary", 2.0));
    state.untrack(&account);

    assert!(state.portfolio(&account).is_none());
    assert!(state.snapshot().is_empty());
}

#[test]
fn state_builds_owner_scoped_snapshots() {
    let mut state = PortfolioState::new();
    let alice = Fixture::account("alice-account");
    let bob = Fixture::account("bob-account");
    state.track_for_owner("alice", &alice);
    state.track_for_owner("bob", &bob);
    state.apply_update(Fixture::update("alice-account", 1.0));
    state.apply_update(Fixture::update("bob-account", 2.0));

    let alice_portfolios = state.snapshot_for_owner("alice");
    assert_eq!(alice_portfolios.len(), 1);
    assert!(alice_portfolios[&alice.cex_id()].contains_key(alice.account_id()));

    let all = state.snapshot();
    assert_eq!(all[&alice.cex_id()].len(), 2);
}

#[test]
fn state_projects_accounts_for_an_exchange() {
    let mut state = PortfolioState::new();
    let first = AccountKey::new(stock_trek_types::cex::cex_id::CexId::Binance, "first");
    let second = AccountKey::new(stock_trek_types::cex::cex_id::CexId::Binance, "second");
    state.track(&first);
    state.track(&second);

    let accounts = state.accounts_for_cex(first.cex_id());
    assert_eq!(accounts.len(), 2);
    assert!(accounts.contains(first.account_id()));
}

#[test]
fn state_ignores_owner_scoped_updates_for_other_owners() {
    let mut state = PortfolioState::new();
    let account = Fixture::account("primary");
    state.track_for_owner("alice", &account);
    let update = crate::PortfolioUpdate::versioned(
        account.clone(),
        Some("bob".to_string()),
        Fixture::portfolio(AssetId::Bitcoin, 3.0),
        1,
        chrono::Utc::now(),
    );

    assert_eq!(
        state.apply_update_with_outcome(update),
        crate::PortfolioUpdateOutcome::IgnoredUntracked
    );
    assert!(state.portfolio(&account).is_none());
}

#[test]
fn state_rejects_updates_that_are_not_newer_than_the_stored_version() {
    let mut state = PortfolioState::new();
    let account = Fixture::account("primary");
    state.track(&account);
    let newer = crate::PortfolioUpdate::versioned(
        account.clone(),
        None,
        Fixture::portfolio(AssetId::Bitcoin, 3.0),
        2,
        chrono::Utc::now(),
    );
    let older = crate::PortfolioUpdate::versioned(
        account,
        None,
        Fixture::portfolio(AssetId::Bitcoin, 1.0),
        1,
        chrono::Utc::now(),
    );

    assert_eq!(
        state.apply_update_with_outcome(newer),
        crate::PortfolioUpdateOutcome::Applied
    );
    assert_eq!(
        state.apply_update_with_outcome(older),
        crate::PortfolioUpdateOutcome::IgnoredStale
    );
    assert_eq!(
        state.snapshot()[&stock_trek_types::cex::cex_id::CexId::Binance]
            [&stock_trek_types::cex::account_id::AccountId("primary".to_string())]
            .asset_total(AssetId::Bitcoin),
        3.0
    );
}
