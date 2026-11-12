use super::{
    BinancePortfolioConfig, BinancePortfolioConnector,
    credentials::BinanceCredentialSource,
    transport::{
        AccountPositionEvent, AccountSnapshot, BinanceAccountEvent, BinanceEventStream,
        BinanceTransport,
    },
};
use crate::portfolio::{
    NoopConnectorHealthSink, PortfolioConnector, PortfolioUpdate, PortfolioUpdateApply,
    PortfolioUpdateSink,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use secrecy::SecretString;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};
use tokio::sync::mpsc;

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
    created: AtomicBool,
}

#[async_trait]
impl BinanceTransport for Transport {
    async fn create_listen_key(&self, _credentials: &ApiKeyCredentials) -> BpResult<String> {
        self.created.store(true, Ordering::SeqCst);
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

struct Sink {
    updates: mpsc::UnboundedSender<PortfolioUpdate>,
}

#[async_trait]
impl PortfolioUpdateSink for Sink {
    async fn apply_update(&self, update: PortfolioUpdate) -> BpResult<PortfolioUpdateApply> {
        self.updates.send(update).expect("receiver");
        Ok(PortfolioUpdateApply::Applied)
    }
}

fn config() -> BinancePortfolioConfig {
    BinancePortfolioConfig::new(
        "https://api.binance.com/api/v3",
        "wss://stream.binance.com:9443/ws",
        Duration::from_secs(3600),
        Duration::from_millis(10),
    )
    .expect("config")
}

#[tokio::test]
async fn connector_streams_snapshot_and_position_updates_to_the_sink() {
    let transport = Arc::new(Transport {
        closes: AtomicUsize::new(0),
        created: AtomicBool::new(false),
    });
    let connector = Arc::new(
        BinancePortfolioConnector::new(
            config(),
            Arc::new(Credentials),
            Arc::new(NoopConnectorHealthSink::new()),
        )
        .with_transport(transport.clone()),
    );
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let connection = connector
        .connect(CexId::Binance, Arc::new(Sink { updates: sender }))
        .await
        .expect("connect");
    connection
        .update_accounts(BTreeSet::from([AccountId::new("primary")]))
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

    assert!(transport.created.load(Ordering::SeqCst));
    connection.disconnect().await.expect("disconnect");
    assert_eq!(transport.closes.load(Ordering::SeqCst), 1);
}
