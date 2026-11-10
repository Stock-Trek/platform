use crate::{JwksCache, JwtSigningKey, OrderJwtClaims};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::jwk::JwkSet;
use std::{
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration as StdDuration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
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
    task: JoinHandle<()>,
}

impl JwksServer {
    async fn start(jwks: &JwkSet) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = Arc::new(RwLock::new(serde_json::to_string(jwks).unwrap()));
        let requests = Arc::new(AtomicUsize::new(0));
        let task = {
            let body = body.clone();
            let requests = requests.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let body = body.clone();
                    let requests = requests.clone();
                    tokio::spawn(async move {
                        let mut request = [0_u8; 2048];
                        let _ = stream.read(&mut request).await;
                        requests.fetch_add(1, Ordering::SeqCst);
                        let body = body.read().unwrap().clone();
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
            task,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/.well-known/jwks.json", self.address)
    }

    fn set_jwks(&self, jwks: &JwkSet) {
        *self.body.write().unwrap() = serde_json::to_string(jwks).unwrap();
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
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
async fn uses_a_stale_cache_when_the_jwks_endpoint_is_unavailable() {
    let signer = Fixture::signer("issuer-a");
    let server = JwksServer::start(signer.jwks()).await;
    let cache = JwksCache::with_ttl(server.url(), StdDuration::from_millis(1)).unwrap();
    let now = Utc::now();
    let issued = signer.sign(&Fixture::claims(now)).unwrap();
    cache.verify(issued.token(), now).await.unwrap();
    drop(server);
    tokio::time::sleep(StdDuration::from_millis(10)).await;

    cache.verify(issued.token(), now).await.unwrap();
}
