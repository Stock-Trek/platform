use super::{
    ExchangePortfolioFeed, PortfolioConnection, PortfolioConnector, PortfolioFeed, PortfolioUpdate,
    PortfolioUpdateApply, PortfolioUpdateSink,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::BpResult;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Debug, Default)]
struct FakeState {
    connects: AtomicUsize,
    disconnects: AtomicUsize,
    updates: Mutex<Vec<BTreeSet<AccountId>>>,
}

impl FakeState {
    fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }

    fn disconnects(&self) -> usize {
        self.disconnects.load(Ordering::SeqCst)
    }

    fn updates(&self) -> Vec<BTreeSet<AccountId>> {
        self.updates.lock().clone()
    }
}

struct FakeConnector {
    state: Arc<FakeState>,
}

impl FakeConnector {
    fn feed() -> (Arc<FakeState>, ExchangePortfolioFeed) {
        let state = Arc::new(FakeState::default());
        let connector = Arc::new(Self {
            state: Arc::clone(&state),
        });
        let feed = ExchangePortfolioFeed::new(connector);
        feed.set_update_sink(Arc::new(NoopSink));
        (state, feed)
    }
}

#[async_trait]
impl PortfolioConnector for FakeConnector {
    async fn connect(
        &self,
        _cex_id: CexId,
        _sink: Arc<dyn PortfolioUpdateSink>,
    ) -> BpResult<Arc<dyn PortfolioConnection>> {
        self.state.connects.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(FakeConnection {
            state: Arc::clone(&self.state),
        }))
    }
}

struct FakeConnection {
    state: Arc<FakeState>,
}

#[async_trait]
impl PortfolioConnection for FakeConnection {
    async fn update_accounts(&self, accounts: BTreeSet<AccountId>) -> BpResult<()> {
        self.state.updates.lock().push(accounts);
        Ok(())
    }

    async fn disconnect(&self) -> BpResult<()> {
        self.state.disconnects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct NoopSink;

#[async_trait]
impl PortfolioUpdateSink for NoopSink {
    async fn apply_update(&self, _update: PortfolioUpdate) -> BpResult<PortfolioUpdateApply> {
        Ok(PortfolioUpdateApply::Applied)
    }
}

#[tokio::test]
async fn exchange_feed_reuses_one_connection_per_exchange() {
    let (state, feed) = FakeConnector::feed();
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("first")]))
        .await
        .expect("first");
    feed.set_accounts(
        CexId::Binance,
        BTreeSet::from([AccountId::new("first"), AccountId::new("second")]),
    )
    .await
    .expect("second");

    assert_eq!(state.connects(), 1);
    assert_eq!(feed.connection_count().await, 1);
    assert_eq!(state.updates().len(), 2);
    assert_eq!(
        state.updates()[1],
        BTreeSet::from([AccountId::new("first"), AccountId::new("second")])
    );
}

#[tokio::test]
async fn exchange_feed_disconnects_an_exchange_without_accounts() {
    let (state, feed) = FakeConnector::feed();
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("first")]))
        .await
        .expect("subscribe");
    feed.set_accounts(CexId::Binance, BTreeSet::new())
        .await
        .expect("unsubscribe");

    assert_eq!(state.disconnects(), 1);
    assert_eq!(feed.connection_count().await, 0);
}

#[tokio::test]
async fn exchange_feed_reconnects_after_an_exchange_is_removed() {
    let (state, feed) = FakeConnector::feed();
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("first")]))
        .await
        .expect("subscribe");
    feed.set_accounts(CexId::Binance, BTreeSet::new())
        .await
        .expect("unsubscribe");
    feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("second")]))
        .await
        .expect("resubscribe");

    assert_eq!(state.connects(), 2);
    assert_eq!(feed.connection_count().await, 1);
}

#[tokio::test]
async fn exchange_feed_requires_a_configured_sink() {
    let state = Arc::new(FakeState::default());
    let connector = Arc::new(FakeConnector {
        state: Arc::clone(&state),
    });
    let feed = ExchangePortfolioFeed::new(connector);
    assert!(
        feed.set_accounts(CexId::Binance, BTreeSet::from([AccountId::new("first")]))
            .await
            .is_err()
    );
    assert_eq!(state.connects(), 0);
}
