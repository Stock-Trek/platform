use super::{
    AccountPositionEvent, AccountSnapshot, BinanceAccountEvent, BinanceEventStream, BinanceSession,
    BinanceTransport,
};
use crate::api_key_credential::ApiKeyCredentials;
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BpResult;
use secrecy::SecretString;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId};

struct Transport {
    closes: AtomicUsize,
    refreshes: AtomicUsize,
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
        self.refreshes.fetch_add(1, Ordering::SeqCst);
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
            events: VecDeque::from([
                BinanceAccountEvent::Position(AccountPositionEvent::new(
                    42,
                    BTreeMap::from([(AssetId::TetherUSD, 500.0)]),
                    Utc::now() + chrono::Duration::seconds(1),
                )),
                BinanceAccountEvent::ListenKeyExpired,
            ]),
        }))
    }
}

struct Stream {
    events: VecDeque<BinanceAccountEvent>,
}

#[async_trait]
impl BinanceEventStream for Stream {
    async fn next(&mut self) -> BpResult<Option<BinanceAccountEvent>> {
        Ok(self.events.pop_front())
    }
}

struct Fixture;

impl Fixture {
    fn credentials() -> ApiKeyCredentials {
        ApiKeyCredentials {
            api_key: "api-key".to_string(),
            secret: SecretString::from("secret"),
        }
    }

    fn transport() -> Arc<Transport> {
        Arc::new(Transport {
            closes: AtomicUsize::new(0),
            refreshes: AtomicUsize::new(0),
            created: AtomicBool::new(false),
        })
    }
}

#[tokio::test]
async fn session_opens_snapshots_refreshes_and_closes_the_listen_key() {
    let transport = Fixture::transport();
    let session = BinanceSession::open(
        Arc::clone(&transport) as Arc<dyn BinanceTransport>,
        &AccountId::new("primary"),
        &Fixture::credentials(),
    )
    .await
    .expect("session");
    let snapshot = session.snapshot().await.expect("snapshot");
    assert_eq!(snapshot.balances().get(&AssetId::Bitcoin), Some(&1.0));
    session.refresh().await.expect("refresh");
    assert!(transport.created.load(Ordering::SeqCst));
    assert_eq!(transport.refreshes.load(Ordering::SeqCst), 1);

    let mut session = session;
    let BinanceAccountEvent::Position(position) =
        session.next().await.expect("event").expect("event")
    else {
        panic!("expected position");
    };
    assert_eq!(position.version(), 42);
    assert_eq!(position.balances().get(&AssetId::TetherUSD), Some(&500.0));
    assert_eq!(
        session.next().await.expect("event"),
        Some(BinanceAccountEvent::ListenKeyExpired)
    );
    assert_eq!(session.next().await.expect("event"), None);

    session.close().await.expect("close");
    assert_eq!(transport.closes.load(Ordering::SeqCst), 1);
}
