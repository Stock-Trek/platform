use crate::{JwksCache, OrderJwtClaims, jwt_signing_key::JwtSigningKey};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::jwk::JwkSet;
use parking_lot::RwLock;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration as StdDuration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};

const OWNER: &str = "cache-owner";

struct Fixture;

impl Fixture {
    fn claims(now: DateTime<Utc>) -> OrderJwtClaims {
        OrderJwtClaims::new("token-a", OWNER, now, now + Duration::minutes(5)).unwrap()
    }

    fn signer(key_id: &str) -> JwtSigningKey {
        JwtSigningKey::generate(key_id).unwrap()
    }
}

struct JwksServer {
    address: SocketAddr,
    body: Arc<RwLock<String>>,
    requests: Arc<AtomicUsize>,
    started: Arc<Notify>,
    gate: Option<Arc<Notify>>,
    task: JoinHandle<()>,
}

impl JwksServer {
    async fn start(jwks: &JwkSet) -> Self {
        Self::start_with_gate(jwks, None).await
    }

    async fn start_gated(jwks: &JwkSet) -> Self {
        Self::start_with_gate(jwks, Some(Arc::new(Notify::new()))).await
    }

    async fn start_with_gate(jwks: &JwkSet, gate: Option<Arc<Notify>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = Arc::new(RwLock::new(serde_json::to_string(jwks).unwrap()));
        let requests = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let task_gate = gate.clone();
        let task = {
            let body = body.clone();
            let requests = requests.clone();
            let started = started.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let body = body.clone();
                    let requests = requests.clone();
                    let started = started.clone();
                    let gate = task_gate.clone();
                    tokio::spawn(async move {
                        let mut request = [0_u8; 2048];
                        let _ = stream.read(&mut request).await;
                        requests.fetch_add(1, Ordering::SeqCst);
                        if let Some(gate) = gate {
                            started.notify_one();
                            gate.notified().await;
                        }
                        let body = body.read().clone();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.shutdown().await;
                    });
                }
            })
        };
        Self {
            address,
            body,
            requests,
            started,
            gate,
            task,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/.well-known/jwks.json", self.address)
    }

    fn set_jwks(&self, jwks: &JwkSet) {
        *self.body.write() = serde_json::to_string(jwks).unwrap();
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    async fn wait_for_request(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        if let Some(gate) = &self.gate {
            gate.notify_waiters();
        }
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn caches_jwks_between_verifications() {
    let signer = Fixture::signer("issuer-a");
    let server = JwksServer::start(signer.jwks()).await;
    let cache = JwksCache::with_ttl(server.url(), StdDuration::from_secs(60)).unwrap();
    let now = Utc::now();
    let issued = signer.sign(&Fixture::claims(now)).unwrap();

    cache.verify(issued.token(), now).await.unwrap();
    cache.verify(issued.token(), now).await.unwrap();

    assert_eq!(server.requests(), 1);
}

#[tokio::test]
async fn refreshes_jwks_when_a_token_references_a_new_key() {
    let first = Fixture::signer("issuer-a");
    let second = Fixture::signer("issuer-b");
    let server = JwksServer::start(first.jwks()).await;
    let cache = JwksCache::with_ttl(server.url(), StdDuration::from_secs(60)).unwrap();
    let now = Utc::now();
    let first_token = first.sign(&Fixture::claims(now)).unwrap();

    cache.verify(first_token.token(), now).await.unwrap();
    server.set_jwks(second.jwks());
    let second_token = second.sign(&Fixture::claims(now)).unwrap();
    cache.verify(second_token.token(), now).await.unwrap();

    assert_eq!(server.requests(), 2);
}

#[tokio::test]
async fn concurrent_verifications_share_a_single_refresh() {
    let signer = Fixture::signer("issuer-a");
    let server = JwksServer::start_gated(signer.jwks()).await;
    let cache = Arc::new(JwksCache::with_ttl(server.url(), StdDuration::from_secs(60)).unwrap());
    let token = signer
        .sign(&Fixture::claims(Utc::now()))
        .unwrap()
        .token()
        .to_string();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let cache = cache.clone();
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            cache.verify(&token, Utc::now()).await
        }));
    }

    server.wait_for_request().await;
    assert_eq!(server.requests(), 1);
    server.release();
    for task in tasks {
        assert!(task.await.unwrap().is_ok());
    }
    assert_eq!(server.requests(), 1);
}

#[tokio::test]
async fn uses_a_stale_cache_when_the_jwks_endpoint_is_unavailable() {
    let signer = Fixture::signer("issuer-a");
    let server = JwksServer::start(signer.jwks()).await;
    let cache = JwksCache::with_ttl(server.url(), StdDuration::from_millis(1)).unwrap();
    let now = Utc::now();
    let issued = signer.sign(&Fixture::claims(now)).unwrap();
    cache.verify(issued.token(), now).await.unwrap();
    drop(server);
    tokio::time::pause();
    tokio::time::advance(StdDuration::from_millis(10)).await;

    cache.verify(issued.token(), now).await.unwrap();
}
