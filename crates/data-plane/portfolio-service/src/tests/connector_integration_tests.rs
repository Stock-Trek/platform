use super::order_request_fixture_tests::{Fixture as OrderFixture, OWNER};
use super::test_feed::Fixture as PortfolioFixture;
use crate::{OrderRequestService, PortfolioService, PortfolioUpdate};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use platform_errors::BpResult;
use platform_exchange_gateway::portfolio::{
    ExchangePortfolioFeed, NoopConnectorHealthSink, PortfolioConnector, PortfolioFeed,
    PortfolioUpdate as GatewayPortfolioUpdate, PortfolioUpdateApply, PortfolioUpdateSink,
    binance::{
        AccountPositionEvent, AccountSnapshot, BinanceAccountEvent, BinanceCredentialSource,
        BinanceEventStream, BinancePortfolioConfig, BinancePortfolioConnector, BinanceTransport,
    },
};
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
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
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId};

struct Credentials;

#[async_trait]
impl BinanceCredentialSource for Credentials {
    async fn credentials(&self, _account_id: &AccountId) -> BpResult<ApiKeyCredentials> {
        Ok(ApiKeyCredentials {
            api_key: "api-key".to_string(),
            secret: SecretString::from("secret"),
        })
    }
}

struct Stream {
    events: VecDeque<BinanceAccountEvent>,
}

#[async_trait]
impl BinanceEventStream for Stream {
    async fn next(&mut self) -> BpResult<Option<BinanceAccountEvent>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        std::future::pending().await
    }
}

struct Transport {
    closes: AtomicUsize,
}

#[async_trait]
impl BinanceTransport for Transport {
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

    async fn open_stream(&self, _listen_key: &str) -> BpResult<Box<dyn BinanceEventStream>> {
        Ok(Box::new(Stream {
            events: VecDeque::from([BinanceAccountEvent::Position(AccountPositionEvent::new(
                42,
                BTreeMap::from([(AssetId::TetherUSD, 500.0)]),
                Utc::now() + chrono::Duration::seconds(1),
            ))]),
        }))
    }
}

struct ServiceSink {
    service: Arc<PortfolioService>,
    updates: Mutex<Vec<PortfolioUpdate>>,
}

#[async_trait]
impl PortfolioUpdateSink for ServiceSink {
    async fn apply_update(&self, update: GatewayPortfolioUpdate) -> BpResult<PortfolioUpdateApply> {
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

#[tokio::test]
async fn connector_updates_authorize_orders_before_and_after_recovery() {
    let transport = Arc::new(Transport {
        closes: AtomicUsize::new(0),
    });
    let connector = Arc::new(
        BinancePortfolioConnector::new(
            BinancePortfolioConfig::new(
                "https://api.binance.com/api/v3",
                "wss://stream.binance.com:9443/ws",
                Duration::from_secs(3600),
                Duration::from_millis(10),
            )
            .expect("config"),
            Arc::new(Credentials),
            Arc::new(NoopConnectorHealthSink::new()),
        )
        .with_transport(transport.clone()),
    );
    let feed = Arc::new(ExchangePortfolioFeed::new(
        connector as Arc<dyn PortfolioConnector>,
    ));
    let service = Arc::new(PortfolioService::new(feed.clone()));
    let sink = Arc::new(ServiceSink {
        service: Arc::clone(&service),
        updates: Mutex::new(Vec::new()),
    });
    feed.set_update_sink(sink.clone());

    service
        .apply_ownership_event(&ownership_event())
        .await
        .expect("ownership");
    wait_for_update(&service).await;

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
        platform_exchange_gateway::portfolio::NoopPortfolioFeed::new(),
    )));
    restarted
        .apply_ownership_event(&ownership_event())
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
    assert_eq!(transport.closes.load(Ordering::SeqCst), 0);
}
