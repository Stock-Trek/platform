use crate::connector::portfolio::{
    ConnectorPortfolioUpdate, ExchangePortfolioConnector, NoopConnectorHealthSink,
    PortfolioAccounts, PortfolioCredentialSource, PortfolioExchange, PortfolioFeed,
    PortfolioUpdateOutcome, PortfolioUpdateSink,
    portfolio_test_fakes_tests::{Fixture, TestTransport},
};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};
use tokio::sync::mpsc;

struct Sink {
    updates: mpsc::UnboundedSender<ConnectorPortfolioUpdate>,
}

#[async_trait]
impl PortfolioUpdateSink for Sink {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> PlatformResult<PortfolioUpdateOutcome> {
        self.updates.send(update).expect("receiver");
        Ok(PortfolioUpdateOutcome::Applied)
    }
}

struct FeedFixture;

impl FeedFixture {
    fn credentials() -> Arc<dyn PortfolioCredentialSource> {
        Arc::new(PortfolioAccounts::new([(
            (CexId::Binance, Fixture::account()),
            ApiKeyCredentials {
                api_key: "api-key".to_string(),
                secret: secrecy::SecretString::from("secret"),
            },
        )]))
    }

    fn feed(transport: Arc<TestTransport>) -> Arc<dyn PortfolioFeed> {
        ExchangePortfolioConnector::new(
            [transport as Arc<dyn PortfolioExchange>],
            Self::credentials(),
            Arc::new(NoopConnectorHealthSink::new()),
        )
        .feed()
    }
}

#[tokio::test]
async fn connector_streams_snapshot_and_position_updates_to_the_sink() {
    let mut persistent = TestTransport::new(Duration::from_secs(3600), Duration::from_millis(10));
    persistent.events.truncate(1);
    let transport = Arc::new(persistent);
    let feed = FeedFixture::feed(Arc::clone(&transport));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    feed.set_update_sink(Arc::new(Sink { updates: sender }));
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("primary")]))
        .await
        .expect("subscribe");

    let snapshot = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("snapshot timeout")
        .expect("snapshot");
    assert_eq!(snapshot.account_id().0, "primary");
    assert_eq!(snapshot.balances().get(&AssetId::Bitcoin), Some(&1.0));
    assert!(snapshot.version() > 0);

    let position = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("position timeout")
        .expect("position");
    assert_eq!(position.balances().get(&AssetId::Bitcoin), Some(&1.0));
    assert_eq!(position.balances().get(&AssetId::TetherUSD), Some(&500.0));
    assert!(position.version() > snapshot.version());

    feed.set_accounts(CexId::Binance, BTreeSet::new())
        .await
        .expect("unsubscribe");
    tokio::time::timeout(Duration::from_secs(2), transport.wait_for_revoke())
        .await
        .expect("revoke timeout");
}

#[tokio::test]
async fn connector_rejects_an_unconfigured_exchange() {
    let feed = FeedFixture::feed(
        TestTransport::new(Duration::from_secs(3600), Duration::from_millis(10)).into(),
    );
    feed.set_update_sink(Arc::new(Sink {
        updates: mpsc::unbounded_channel().0,
    }));
    let error = ExchangePortfolioConnector::new(
        [],
        FeedFixture::credentials(),
        Arc::new(NoopConnectorHealthSink::new()),
    )
    .feed()
    .set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("primary")]))
    .await;
    assert!(error.is_err());
    assert!(
        feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("primary")]))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn connector_reopens_a_session_after_expiration() {
    let transport = Arc::new(TestTransport::new(
        Duration::from_secs(3600),
        Duration::from_millis(10),
    ));
    let feed = FeedFixture::feed(Arc::clone(&transport));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    feed.set_update_sink(Arc::new(Sink { updates: sender }));
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("primary")]))
        .await
        .expect("subscribe");

    tokio::time::timeout(Duration::from_secs(2), async {
        while transport.opened() < 2 {
            assert!(receiver.recv().await.is_some());
        }
    })
    .await
    .expect("reconnect timeout");

    feed.set_accounts(CexId::Binance, BTreeSet::new())
        .await
        .expect("unsubscribe");
}
