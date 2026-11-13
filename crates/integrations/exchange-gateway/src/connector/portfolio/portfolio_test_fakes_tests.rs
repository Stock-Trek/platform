use crate::connector::portfolio::{PortfolioEvent, PortfolioExchange, PortfolioStream};
use chrono::Utc;
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    encode::ByteEncoder,
    portfolio::{
        AccountEvent, AccountSnapshot, AuthBinder, AuthError, Credential, CredentialProvider,
        PortfolioAuth, RenewalSchedule, SessionLifecycle,
    },
    signer::Signer,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};

pub struct TestCredentialProvider {
    pub acquired: Arc<AtomicUsize>,
    pub renewed: Arc<AtomicUsize>,
    pub revoked: Arc<AtomicUsize>,
    pub ttl: Duration,
}

#[async_trait::async_trait]
impl CredentialProvider for TestCredentialProvider {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError> {
        self.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Self::credential(self.ttl)))
    }

    async fn renew(&self, _credential: &Credential) -> Result<Credential, AuthError> {
        self.renewed.fetch_add(1, Ordering::SeqCst);
        Ok(Self::credential(self.ttl))
    }

    async fn revoke(&self, _credential: &Credential) -> Result<(), AuthError> {
        self.revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl TestCredentialProvider {
    fn credential(ttl: Duration) -> Credential {
        let issued_at = Instant::now();
        Credential {
            value: "credential".to_string(),
            issued_at,
            expires_at: Some(issued_at + ttl),
            renewable: true,
        }
    }
}

#[derive(Default)]
pub struct TestAuthBinder;

#[async_trait::async_trait]
impl AuthBinder for TestAuthBinder {
    fn websocket_url(
        &self,
        url: &str,
        _credential: Option<&Credential>,
        _signer: &Signer,
    ) -> Result<String, AuthError> {
        Ok(url.to_string())
    }
}

#[derive(Debug)]
struct TestListener {
    _events: tokio::sync::mpsc::UnboundedSender<PortfolioEvent>,
}

#[async_trait::async_trait]
impl iris::Listener<serde_json::Value> for TestListener {
    async fn on_message(&self, _message: serde_json::Value) {}
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

    fn stream(events: &[AccountEvent]) -> PortfolioStream {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        for event in events {
            let _ = sender.send(PortfolioEvent::Account(event.clone()));
        }
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

#[async_trait::async_trait]
impl PortfolioExchange for TestTransport {
    fn cex_id(&self) -> CexId {
        CexId::Binance
    }

    fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    async fn portfolio_auth(&self, _credentials: &ApiKeyCredentials) -> BuildResult<PortfolioAuth> {
        Ok(PortfolioAuth::new(
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
        ))
    }

    async fn account_snapshot(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> BuildResult<AccountSnapshot> {
        Ok(AccountSnapshot::new(self.snapshot.clone(), Utc::now()))
    }

    async fn open_stream(
        &self,
        _auth: &PortfolioAuth,
        _credential: Option<&Credential>,
    ) -> BuildResult<PortfolioStream> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        Ok(Self::stream(&self.events))
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
