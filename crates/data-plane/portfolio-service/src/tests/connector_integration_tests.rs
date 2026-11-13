use crate::tests::order_request_fixture_tests::{Fixture as OrderFixture, OWNER};
use crate::tests::test_feed_tests::Fixture as PortfolioFixture;
use crate::{OrderRequestService, PortfolioService, PortfolioUpdate};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use platform_errors::BuildResult;
use platform_exchange_gateway::{
    ConnectorPortfolioUpdate, ExchangePortfolioConnector, NoopConnectorHealthSink,
    NoopPortfolioFeed, PortfolioCredentialSource, PortfolioEvent, PortfolioExchange, PortfolioFeed,
    PortfolioStream, PortfolioUpdateApply, PortfolioUpdateSink, iris,
};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    encode::ByteEncoder,
    portfolio::{
        AccountEvent, AccountPositionEvent, AccountSnapshot, AuthBinder, AuthError, Credential,
        CredentialProvider, PortfolioAuth, RenewalSchedule, SessionLifecycle,
    },
    signer::Signer,
};
use platform_portfolio_ownership::{OwnershipAction, OwnershipEvent};
use secrecy::SecretString;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};
use tokio::sync::Notify;

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

struct TestCredentialProvider {
    closes: Arc<AtomicUsize>,
}

#[async_trait]
impl CredentialProvider for TestCredentialProvider {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError> {
        let issued_at = Instant::now();
        Ok(Some(Credential {
            value: "listen-key".to_string(),
            issued_at,
            expires_at: Some(issued_at + Duration::from_hours(1)),
            renewable: true,
        }))
    }

    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError> {
        Ok(credential.clone())
    }

    async fn revoke(&self, _credential: &Credential) -> Result<(), AuthError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct TestBinder;

#[async_trait]
impl AuthBinder for TestBinder {}

impl Transport {
    fn stream() -> PortfolioStream {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let _ = sender.send(PortfolioEvent::Account(AccountEvent::Position(
            AccountPositionEvent::new(
                42,
                BTreeMap::from([(AssetId::TetherUSD, 500.0)]),
                Utc::now() + chrono::Duration::seconds(1),
            ),
        )));
        let client = iris::Client::new(
            iris::Config::new(),
            Arc::new(TestListener {
                _events: sender.clone(),
            }),
            "ws://localhost/ws",
        );
        PortfolioStream::new(client, receiver)
    }
}

#[derive(Debug)]
struct TestListener {
    _events: tokio::sync::mpsc::UnboundedSender<PortfolioEvent>,
}

#[async_trait]
impl iris::Listener<serde_json::Value> for TestListener {
    async fn on_message(&self, _message: serde_json::Value) {}
}

#[async_trait]
impl PortfolioExchange for Transport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn reconnect_backoff(&self) -> Duration {
        Duration::from_millis(10)
    }

    async fn portfolio_auth(&self, _credentials: &ApiKeyCredentials) -> BuildResult<PortfolioAuth> {
        Ok(PortfolioAuth::new(
            TestCredentialProvider {
                closes: Arc::clone(&self.closes),
            },
            TestBinder,
            SessionLifecycle::new(
                None,
                Some(RenewalSchedule::new(
                    Duration::from_secs(3600),
                    Duration::ZERO,
                )),
                true,
                true,
            ),
            Signer::new_unencrypted(ByteEncoder::HexLower),
        ))
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

    async fn open_stream(
        &self,
        _auth: &PortfolioAuth,
        _credential: Option<&Credential>,
    ) -> BuildResult<PortfolioStream> {
        Ok(Self::stream())
    }
}

struct ServiceSink {
    service: Arc<PortfolioService>,
    updates: Mutex<Vec<PortfolioUpdate>>,
    applied: Arc<Notify>,
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
        self.applied.notify_one();
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

    async fn wait_for_update(service: &PortfolioService, applied: &Notify) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if service
                    .account_portfolio(&PortfolioFixture::account("account-1"))
                    .is_some_and(|portfolio| portfolio.asset_total(AssetId::TetherUSD) == 500.0)
                {
                    return;
                }
                applied.notified().await;
            }
        })
        .await
        .expect("portfolio update timeout");
    }

    fn feed(closes: Arc<AtomicUsize>) -> Arc<dyn PortfolioFeed> {
        ExchangePortfolioConnector::new(
            [Arc::new(Transport { closes }) as Arc<dyn PortfolioExchange>],
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
        applied: Arc::new(Notify::new()),
    });
    feed.set_update_sink(sink.clone());

    service
        .apply_ownership_event(&Fixture::ownership_event())
        .await
        .expect("ownership");
    Fixture::wait_for_update(&service, &sink.applied).await;

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

    let restarted = Arc::new(PortfolioService::new(Arc::new(NoopPortfolioFeed::new())));
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
