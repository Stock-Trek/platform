use crate::connector::portfolio::{
    PortfolioExchange, PortfolioSession,
    portfolio_test_fakes_tests::{Fixture, TestTransport},
};
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::asset_id::AssetId;

#[tokio::test]
async fn session_opens_snapshots_refreshes_and_revokes_the_credential() {
    let transport = Arc::new(TestTransport::new(
        Duration::from_secs(60),
        Duration::from_secs(1),
    ));
    let mut session = PortfolioSession::open(
        Arc::clone(&transport) as Arc<dyn PortfolioExchange>,
        &Fixture::account(),
        &Fixture::credentials(),
    )
    .await
    .expect("session");
    let snapshot = session.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.balances().get(&AssetId::Bitcoin), Some(&1.0));
    session.refresh().await.expect("refresh");
    assert_eq!(transport.acquired(), 1);
    assert_eq!(transport.renewed(), 1);

    let event = session.next().await.expect("event").expect("event");
    let platform_exchange_types::portfolio::AccountEvent::Position(position) = event else {
        panic!("expected position");
    };
    assert_eq!(position.version(), 42);
    assert_eq!(position.balances().get(&AssetId::TetherUSD), Some(&500.0));

    session.close().await.expect("close");
    assert_eq!(transport.revoked(), 1);
}

#[tokio::test]
async fn session_reuses_a_valid_credential_after_reconnect() {
    let transport = Arc::new(TestTransport::new(
        Duration::from_secs(60),
        Duration::from_secs(1),
    ));
    let session = PortfolioSession::open(
        Arc::clone(&transport) as Arc<dyn PortfolioExchange>,
        &Fixture::account(),
        &Fixture::credentials(),
    )
    .await
    .expect("session");
    let credential = session.credential().cloned();
    let mut reopened = PortfolioSession::open_with_credential(
        Arc::clone(&transport) as Arc<dyn PortfolioExchange>,
        &Fixture::account(),
        &Fixture::credentials(),
        credential,
    )
    .await
    .expect("reopened session");
    assert_eq!(transport.acquired(), 1);
    assert_eq!(transport.opened(), 2);
    reopened.close().await.expect("close");
}
