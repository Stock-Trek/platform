use crate::connector::portfolio::{AccountEventStream, PortfolioEventStream, PortfolioTransport};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    encode::ByteEncoder,
    portfolio::{
        AccountEvent, AccountSnapshot, AuthBinder, AuthError, ConnectRequest, Credential,
        CredentialProvider, PortfolioAuth, RenewalSchedule, SessionLifecycle, WsConnection,
    },
    signer::Signer,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};
use url::Url;

pub struct TestCredentialProvider {
    pub acquired: Arc<AtomicUsize>,
    pub renewed: Arc<AtomicUsize>,
    pub revoked: Arc<AtomicUsize>,
    pub ttl: Duration,
}

#[async_trait]
impl CredentialProvider for TestCredentialProvider {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        let issued_at = Instant::now();
        Ok(Some(Credential {
            value: "credential".to_string(),
            issued_at,
            expires_at: Some(issued_at + self.ttl),
            renewable: true,
        }))
    }

    async fn renew(&self, _credential: &Credential) -> Result<Credential, AuthError> {
        self.renewed.fetch_add(1, Ordering::SeqCst);
        let issued_at = Instant::now();
        Ok(Credential {
            value: "credential".to_string(),
            issued_at,
            expires_at: Some(issued_at + self.ttl),
            renewable: true,
        })
    }

    async fn revoke(&self, _credential: &Credential) -> Result<(), AuthError> {
        self.revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
pub struct TestAuthBinder;

#[async_trait]
impl AuthBinder for TestAuthBinder {
    fn prepare_connect(
        &self,
        _request: &mut ConnectRequest,
        _credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    async fn bind(
        &self,
        _connection: &dyn WsConnection,
        _credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    fn confirmation_predicate(
        &self,
    ) -> Option<platform_exchange_types::portfolio::ConfirmationPredicate> {
        None
    }
}

pub struct TestStream {
    pub events: VecDeque<AccountEvent>,
}

#[async_trait]
impl AccountEventStream for TestStream {
    async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        if let Some(event) = self.events.pop_front() {
            return Ok(Some(event));
        }
        std::future::pending().await
    }
}

#[async_trait]
impl WsConnection for TestStream {
    async fn send_text(&self, _payload: &str) -> Result<(), AuthError> {
        Ok(())
    }

    async fn send_binary(&self, _payload: &[u8]) -> Result<(), AuthError> {
        Ok(())
    }
}

pub struct TestTransport {
    pub acquired: Arc<AtomicUsize>,
    pub renewed: Arc<AtomicUsize>,
    pub revoked: Arc<AtomicUsize>,
    pub opened: Arc<AtomicUsize>,
    pub refresh_interval: Duration,
    pub reconnect_backoff: Duration,
    pub events: Vec<AccountEvent>,
    pub snapshot: BTreeMap<AssetId, f64>,
    pub reconnect_reuses_credential: bool,
}

impl TestTransport {
    pub fn new(refresh_interval: Duration, reconnect_backoff: Duration) -> Self {
        Self {
            acquired: Arc::new(AtomicUsize::new(0)),
            renewed: Arc::new(AtomicUsize::new(0)),
            revoked: Arc::new(AtomicUsize::new(0)),
            opened: Arc::new(AtomicUsize::new(0)),
            refresh_interval,
            reconnect_backoff,
            events: vec![
                AccountEvent::Position(
                    platform_exchange_types::portfolio::AccountPositionEvent::new(
                        42,
                        BTreeMap::from([(AssetId::TetherUSD, 500.0)]),
                        Utc::now() + chrono::Duration::seconds(1),
                    ),
                ),
                AccountEvent::SessionExpired,
            ],
            snapshot: BTreeMap::from([(AssetId::Bitcoin, 1.0)]),
            reconnect_reuses_credential: true,
        }
    }

    pub fn acquired(&self) -> usize {
        self.acquired.load(Ordering::SeqCst)
    }

    pub fn renewed(&self) -> usize {
        self.renewed.load(Ordering::SeqCst)
    }

    pub fn revoked(&self) -> usize {
        self.revoked.load(Ordering::SeqCst)
    }

    pub fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PortfolioTransport for TestTransport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    fn connect_request(&self) -> BuildResult<ConnectRequest> {
        Ok(ConnectRequest::new(
            Url::parse("ws://localhost/ws").expect("url"),
        ))
    }

    async fn adapter(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> BuildResult<Box<dyn platform_exchange_types::portfolio::ExchangeAdapter>> {
        Ok(Box::new(PortfolioAuth::new(
            TestCredentialProvider {
                acquired: Arc::clone(&self.acquired),
                renewed: Arc::clone(&self.renewed),
                revoked: Arc::clone(&self.revoked),
                ttl: Duration::from_hours(1),
            },
            TestAuthBinder,
            SessionLifecycle::new(
                None,
                Some(RenewalSchedule::new(self.refresh_interval, Duration::ZERO)),
                self.reconnect_reuses_credential,
                true,
            ),
            Signer::new_unencrypted(ByteEncoder::HexLower),
        )))
    }

    async fn account_snapshot(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        Ok(AccountSnapshot::new(self.snapshot.clone(), Utc::now()))
    }

    async fn open_stream(
        &self,
        _request: ConnectRequest,
    ) -> BuildResult<Box<dyn PortfolioEventStream>> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestStream {
            events: self.events.clone().into(),
        }))
    }
}

pub struct Fixture;

impl Fixture {
    pub fn credentials() -> ApiKeyCredentials {
        ApiKeyCredentials {
            api_key: "api-key".to_string(),
            secret: secrecy::SecretString::from("secret"),
        }
    }

    pub fn account() -> AccountId {
        AccountId::new("primary")
    }
}
