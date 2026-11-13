use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::portfolio::{
        auth_binder::BinanceAuthBinder, credential_client::BinanceListenKeyClient,
        credential_provider::BinanceCredentialProvider,
        session_lifecycle::BinanceSessionLifecyclePolicy,
    },
    portfolio::{
        AuthBinder, AuthError, ConnectRequest, Credential, CredentialProvider, RenewalSchedule,
        SessionLifecyclePolicy, Signer,
    },
};
use async_trait::async_trait;
use secrecy::SecretString;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use url::Url;

struct TestClient {
    created: Arc<AtomicUsize>,
    renewed: Arc<AtomicUsize>,
    revoked: Arc<AtomicUsize>,
}

#[async_trait]
impl BinanceListenKeyClient for TestClient {
    async fn create_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
    ) -> Result<String, AuthError> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok("listen-key".to_string())
    }

    async fn keepalive_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
        _listen_key: &str,
    ) -> Result<(), AuthError> {
        self.renewed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close_listen_key(
        &self,
        _credentials: &ApiKeyCredentials,
        _listen_key: &str,
    ) -> Result<(), AuthError> {
        self.revoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct TestSigner;

impl Signer for TestSigner {
    fn api_key(&self) -> &str {
        "api-key"
    }

    fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, AuthError> {
        Ok(payload.to_vec())
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

    fn client() -> (
        TestClient,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let created = Arc::new(AtomicUsize::new(0));
        let renewed = Arc::new(AtomicUsize::new(0));
        let revoked = Arc::new(AtomicUsize::new(0));
        (
            TestClient {
                created: Arc::clone(&created),
                renewed: Arc::clone(&renewed),
                revoked: Arc::clone(&revoked),
            },
            created,
            renewed,
            revoked,
        )
    }
}

#[test]
fn binance_binder_appends_the_listen_key_to_the_websocket_path() {
    let mut request = ConnectRequest::new(Url::parse("wss://example.test/ws").expect("url"));
    BinanceAuthBinder::new()
        .prepare_connect(
            &mut request,
            Some(&Credential::new(
                "listen-key",
                std::time::Instant::now(),
                None,
            )),
            &TestSigner,
        )
        .expect("bind");
    assert_eq!(request.url.as_str(), "wss://example.test/ws/listen-key");
}

#[test]
fn binance_binder_requires_a_credential() {
    let mut request = ConnectRequest::new(Url::parse("wss://example.test/ws").expect("url"));
    let error = BinanceAuthBinder::new()
        .prepare_connect(&mut request, None, &TestSigner)
        .expect_err("missing credential");
    assert!(matches!(error, AuthError::Bind(_)));
}

#[tokio::test]
async fn binance_provider_acquires_renews_and_revokes_a_listen_key() {
    let (client, created, renewed, revoked) = Fixture::client();
    let provider = BinanceCredentialProvider::new(client, Fixture::credentials())
        .with_ttl(Duration::from_mins(1));
    let credential = provider
        .acquire()
        .await
        .expect("acquire")
        .expect("credential");
    assert_eq!(credential.value, "listen-key");
    assert_eq!(created.load(Ordering::SeqCst), 1);
    let renewed_credential = provider.renew(&credential).await.expect("renew");
    assert_eq!(renewed_credential.value, "listen-key");
    assert_eq!(renewed.load(Ordering::SeqCst), 1);
    provider.revoke(&renewed_credential).await.expect("revoke");
    assert_eq!(revoked.load(Ordering::SeqCst), 1);
}

#[test]
fn binance_lifecycle_exposes_the_declarative_policy() {
    let policy = BinanceSessionLifecyclePolicy::new(
        Some(Duration::from_hours(24)),
        Some(RenewalSchedule::new(
            Duration::from_mins(30),
            Duration::from_mins(5),
        )),
        true,
        false,
    );
    assert_eq!(
        policy.max_session_lifetime(),
        Some(Duration::from_hours(24))
    );
    assert_eq!(
        policy.scheduled_renewal(),
        Some(RenewalSchedule::new(
            Duration::from_mins(30),
            Duration::from_mins(5)
        ))
    );
    assert!(policy.reconnect_reuses_credential());
    assert!(!policy.revoke_on_close());
}
