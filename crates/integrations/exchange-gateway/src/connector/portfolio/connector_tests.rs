use super::{
    AccountEventStream, ConnectorPortfolioUpdate, ExchangePortfolioConnector,
    NoopConnectorHealthSink, PortfolioCredentialSource, PortfolioFeed, PortfolioTransport,
    PortfolioUpdateApply, PortfolioUpdateSink, StaticPortfolioCredentials,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BpResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{AccountEvent, AccountPositionEvent, AccountSnapshot},
};
use secrecy::SecretString;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};
use tokio::sync::mpsc;

struct Transport {
    opens: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

#[async_trait]
impl PortfolioTransport for Transport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn refresh_interval(&self) -> Duration {
        Duration::from_secs(3600)
    }

    fn reconnect_backoff(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn create_listen_key(&self, _credentials: &ApiKeyCredentials) -> BpResult<String> {
        Ok("listen-key".to_string())
    }

    async fn keepalive_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
        _listen_key: &str,
    ) -> BpResult<()> {
        Ok(())
    }

    async fn close_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
        _listen_key: &str,
    ) -> BpResult<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn account_snapshot(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> BpResult<AccountSnapshot> {
        Ok(AccountSnapshot::new(
            BTreeMap::from([(AssetId::Bitcoin, 1.0)]),
            Utc::now(),
        ))
    }

    async fn open_stream(&self, _listen_key: &str) -> BpResult<Box<dyn AccountEventStream>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Stream {
            events: VecDeque::from([
                AccountEvent::Position(AccountPositionEvent::new(
                    42,
                    BTreeMap::from([(AssetId::TetherUSD, 500.0)]),
                    Utc::now() + chrono::Duration::seconds(1),
                )),
                AccountEvent::SessionExpired,
            ]),
        }))
    }
}

struct Stream {
    events: VecDeque<AccountEvent>,
}

#[async_trait]
impl AccountEventStream for Stream {
    async fn next(&mut self) -> BpResult<Option<AccountEvent>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        std::future::pending().await
    }
}

struct Sink {
    updates: mpsc::UnboundedSender<ConnectorPortfolioUpdate>,
}

#[async_trait]
impl PortfolioUpdateSink for Sink {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> BpResult<PortfolioUpdateApply> {
        self.updates.send(update).expect("receiver");
        Ok(PortfolioUpdateApply::Applied)
    }
}

struct Fixture;

impl Fixture {
    fn credentials() -> Arc<dyn PortfolioCredentialSource> {
        Arc::new(StaticPortfolioCredentials::new([(
            (CexId::Binance, AccountId::new("primary")),
            ApiKeyCredentials {
                api_key: "api-key".to_string(),
                secret: SecretString::from("secret"),
            },
        )]))
    }

    fn feed(transport: Arc<Transport>) -> Arc<dyn PortfolioFeed> {
        ExchangePortfolioConnector::new(
            [transport as Arc<dyn PortfolioTransport>],
            Self::credentials(),
            Arc::new(NoopConnectorHealthSink::new()),
        )
        .feed()
    }
}

#[tokio::test]
async fn connector_streams_snapshot_and_position_updates_to_the_sink() {
    let transport = Arc::new(Transport {
        opens: Arc::new(AtomicUsize::new(0)),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let feed = Fixture::feed(Arc::clone(&transport));
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
    tokio::time::timeout(Duration::from_secs(2), async {
        while transport.closes.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("close timeout");
}

#[tokio::test]
async fn connector_rejects_an_unconfigured_exchange() {
    let transport = Arc::new(Transport {
        opens: Arc::new(AtomicUsize::new(0)),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let feed = Fixture::feed(transport);
    feed.set_update_sink(Arc::new(Sink {
        updates: mpsc::unbounded_channel().0,
    }));
    let error = ExchangePortfolioConnector::new(
        [],
        Fixture::credentials(),
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
    let transport = Arc::new(Transport {
        opens: Arc::new(AtomicUsize::new(0)),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let feed = Fixture::feed(Arc::clone(&transport));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    feed.set_update_sink(Arc::new(Sink { updates: sender }));
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("primary")]))
        .await
        .expect("subscribe");

    tokio::time::timeout(Duration::from_secs(2), async {
        while transport.opens.load(Ordering::SeqCst) < 2 {
            assert!(receiver.recv().await.is_some());
        }
    })
    .await
    .expect("reconnect timeout");

    feed.set_accounts(CexId::Binance, BTreeSet::new())
        .await
        .expect("unsubscribe");
}
