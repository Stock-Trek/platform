use crate::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        exchange::BinanceExchange,
        portfolio::{
            auth_binder::BinanceAuthBinder, credential_provider::BinanceCredentialProvider,
        },
    },
    encode::ByteEncoder,
    http::{HttpClient, HttpMethod, HttpRequest, HttpResponse},
    portfolio::{
        AuthBinder, AuthError, ConnectRequest, Credential, CredentialProvider, RenewalSchedule,
    },
    signer::Signer,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use secrecy::SecretString;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use url::Url;

struct TestHttpClient {
    created: Arc<AtomicUsize>,
    renewed: Arc<AtomicUsize>,
    revoked: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpClient for TestHttpClient {
    async fn send(&self, request: HttpRequest, _timeout: Duration) -> BuildResult<HttpResponse> {
        match request.method {
            HttpMethod::POST => {
                self.created.fetch_add(1, Ordering::SeqCst);
            }
            HttpMethod::PUT => {
                self.renewed.fetch_add(1, Ordering::SeqCst);
            }
            HttpMethod::DELETE => {
                self.revoked.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: br#"{"listenKey":"listen-key"}"#.to_vec(),
        })
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

    fn provider() -> (
        BinanceCredentialProvider,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let created = Arc::new(AtomicUsize::new(0));
        let renewed = Arc::new(AtomicUsize::new(0));
        let revoked = Arc::new(AtomicUsize::new(0));
        (
            BinanceCredentialProvider::new(
                Arc::new(TestHttpClient {
                    created: Arc::clone(&created),
                    renewed: Arc::clone(&renewed),
                    revoked: Arc::clone(&revoked),
                }),
                Self::credentials(),
            ),
            created,
            renewed,
            revoked,
        )
    }

    fn signer() -> Signer {
        Signer::new_unencrypted(ByteEncoder::HexLower)
    }
}

#[test]
fn binance_binder_appends_the_listen_key_to_the_websocket_path() {
    let mut request = ConnectRequest::new(Url::parse("wss://example.test/ws").expect("url"));
    BinanceAuthBinder::new()
        .prepare_connect(
            &mut request,
            Some(&Credential::new("listen-key", Instant::now(), None)),
            &Fixture::signer(),
        )
        .expect("bind");
    assert_eq!(request.url.as_str(), "wss://example.test/ws/listen-key");
}

#[test]
fn binance_binder_requires_a_credential() {
    let mut request = ConnectRequest::new(Url::parse("wss://example.test/ws").expect("url"));
    let error = BinanceAuthBinder::new()
        .prepare_connect(&mut request, None, &Fixture::signer())
        .expect_err("missing credential");
    assert!(matches!(error, AuthError::Bind(_)));
}

#[tokio::test]
async fn binance_provider_acquires_renews_and_revokes_a_listen_key() {
    let (provider, created, renewed, revoked) = Fixture::provider();
    let provider = provider.with_ttl(Duration::from_mins(1));
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
    let lifecycle = BinanceExchange::portfolio_session_lifecycle(Duration::from_mins(30));
    assert_eq!(
        lifecycle.max_session_lifetime(),
        Some(Duration::from_hours(24))
    );
    assert_eq!(
        lifecycle.scheduled_renewal(),
        Some(RenewalSchedule::new(
            Duration::from_mins(30),
            Duration::from_secs(30)
        ))
    );
    assert!(lifecycle.reconnect_reuses_credential());
    assert!(lifecycle.revoke_on_close());
}
