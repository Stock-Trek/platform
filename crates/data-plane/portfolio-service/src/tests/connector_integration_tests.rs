use crate::tests::order_request_fixture_tests::{Fixture as OrderFixture, OWNER};
use crate::tests::test_feed_tests::Fixture as PortfolioFixture;
use crate::{OrderRequestService, PortfolioService, PortfolioUpdate};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use platform_errors::BuildResult;
use platform_exchange_gateway::connector::{
    AccountEventStream, ConnectorPortfolioUpdate, ExchangePortfolioConnector,
    NoopConnectorHealthSink, PortfolioCredentialSource, PortfolioFeed, PortfolioTransport,
    PortfolioUpdateApply, PortfolioUpdateSink,
};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{AccountEvent, AccountPositionEvent, AccountSnapshot},
};
use platform_portfolio_ownership::{OwnershipAction, OwnershipEvent};
use secrecy::SecretString;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};

struct Credentials;

#[async_trait]
impl PortfolioCredentialSource for Credentials {
    async fn credentials(
        &self,
        _cex_id: CexId,
        _account_id: &AccountId,
    ) -> BuildResult<ApiKeyCredentials> {
        Ok(ApiKeyCredentials {
            api_key: "api-key".to_string(),
            secret: SecretString::from("secret"),
        })
    }
}

struct Transport {
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

    async fn create_listen_key(&self, _credentials: &ApiKeyCredentials) -> BuildResult<String> {
        Ok("listen-key".to_string())
    }

    async fn keepalive_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
        _listen_key: &str,
    ) -> BuildResult<()> {
        Ok(())
    }

    async fn close_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
        _listen_key: &str,
    ) -> BuildResult<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn account_snapshot(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        Ok(AccountSnapshot::new(
            BTreeMap::from([(AssetId::Bitcoin, 1.0)]),
            Utc::now(),
        ))
    }

    async fn open_stream(&self, _listen_key: &str) -> BuildResult<Box<dyn AccountEventStream>> {
        Ok(Box::new(Stream {
            events: VecDeque::from([AccountEvent::Position(AccountPositionEvent::new(
                42,
                BTreeMap::from([(AssetId::TetherUSD, 500.0)]),
                Utc::now() + chrono::Duration::seconds(1),
            ))]),
        }))
    }
}

struct Stream {
    events: VecDeque<AccountEvent>,
}

#[async_trait]
impl AccountEventStream for Stream {
    async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        std::future::pending().await
    }
}

struct ServiceSink {
    service: Arc<PortfolioService>,
    updates: Mutex<Vec<PortfolioUpdate>>,
}

#[async_trait]
impl PortfolioUpdateSink for ServiceSink {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> BuildResult<PortfolioUpdateApply> {
        let update = PortfolioUpdate::try_from(update)?;
        let outcome = self.service.apply_portfolio_update(update.clone());
        self.updates.lock().push(update);
        Ok(match outcome {
            crate::PortfolioUpdateOutcome::Applied => PortfolioUpdateApply::Applied,
            crate::PortfolioUpdateOutcome::IgnoredUntracked => {
                PortfolioUpdateApply::IgnoredUntracked
            }
            crate::PortfolioUpdateOutcome::IgnoredStale => PortfolioUpdateApply::IgnoredStale,
        })
    }
}

struct Fixture;

impl Fixture {
    fn ownership_event() -> OwnershipEvent {
        OwnershipEvent::new(
            "audit-1",
            OWNER,
            PortfolioFixture::account("account-1"),
            OwnershipAction::Add,
            1,
            Utc::now(),
        )
    }

    async fn wait_for_update(service: &PortfolioService) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if service
                    .account_portfolio(&PortfolioFixture::account("account-1"))
                    .is_some_and(|portfolio| portfolio.asset_total(AssetId::TetherUSD) == 500.0)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("portfolio update timeout");
    }

    fn feed(closes: Arc<AtomicUsize>) -> Arc<dyn PortfolioFeed> {
        ExchangePortfolioConnector::new(
            [Arc::new(Transport { closes }) as Arc<dyn PortfolioTransport>],
            Arc::new(Credentials),
            Arc::new(NoopConnectorHealthSink::new()),
        )
        .feed()
    }
}

#[tokio::test]
async fn connector_updates_authorize_orders_before_and_after_recovery() {
    let closes = Arc::new(AtomicUsize::new(0));
    let feed = Fixture::feed(Arc::clone(&closes));
    let service = Arc::new(PortfolioService::new(Arc::clone(&feed)));
    let sink = Arc::new(ServiceSink {
        service: Arc::clone(&service),
        updates: Mutex::new(Vec::new()),
    });
    feed.set_update_sink(sink.clone());

    service
        .apply_ownership_event(&Fixture::ownership_event())
        .await
        .expect("ownership");
    Fixture::wait_for_update(&service).await;

    let (_, auth_service, _) = OrderFixture::service().await;
    let order_service =
        OrderRequestService::new(auth_service.clone()).with_portfolio_service(Arc::clone(&service));
    OrderFixture::sign(
        &order_service,
        &auth_service,
        &OrderFixture::unsigned(OWNER),
    )
    .await
    .expect("order before recovery");

    let restarted = Arc::new(PortfolioService::new(Arc::new(
        platform_exchange_gateway::connector::NoopPortfolioFeed::new(),
    )));
    restarted
        .apply_ownership_event(&Fixture::ownership_event())
        .await
        .expect("ownership replay");
    for update in sink.updates.lock().clone() {
        assert_eq!(
            restarted.apply_portfolio_update(update),
            crate::PortfolioUpdateOutcome::Applied
        );
    }
    let recovered =
        OrderRequestService::new(auth_service.clone()).with_portfolio_service(restarted);
    OrderFixture::sign(&recovered, &auth_service, &OrderFixture::unsigned(OWNER))
        .await
        .expect("order after recovery");
    assert_eq!(closes.load(Ordering::SeqCst), 0);
}
