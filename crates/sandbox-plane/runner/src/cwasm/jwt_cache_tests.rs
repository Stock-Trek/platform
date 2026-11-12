use super::JwtCache;
use platform_sigv4::{SigV4Config, SigV4Registry};
use std::{
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

const OWNER: &str = "cache-owner";

struct Fixture;

impl Fixture {
    fn sigv4() -> SigV4Config {
        let registry = SigV4Registry::from_parts("AKID", "secret", OWNER, None).unwrap();
        SigV4Config::new(registry, "eu-west-1", "execute-api").unwrap()
    }

    fn cache(url: &str) -> JwtCache {
        JwtCache::with_sigv4(url, OWNER, &Self::sigv4()).unwrap()
    }

    fn jwt_response(token: &str, lifetime_secs: u64) -> String {
        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + lifetime_secs;
        format!(r#"{{"token":"{token}","expiresAtUnix":{expires_at}}}"#)
    }
}

struct AuthServer {
    address: SocketAddr,
    body: Arc<RwLock<String>>,
    requests: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl AuthServer {
    async fn start(body: String) -> Self {
        Self::start_with_delay(body, Duration::ZERO).await
    }

    async fn start_with_delay(body: String, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = Arc::new(RwLock::new(body));
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
                        tokio::time::sleep(delay).await;
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
        format!("http://{}/jwt", self.address)
    }

    fn set_body(&self, body: String) {
        *self.body.write().unwrap() = body;
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for AuthServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn reuses_a_fresh_token() {
    let server = AuthServer::start(Fixture::jwt_response("token-a", 3600)).await;
    let cache = Fixture::cache(&server.url());

    assert_eq!(cache.token().await.unwrap(), "token-a");
    assert_eq!(cache.token().await.unwrap(), "token-a");
    assert_eq!(server.requests(), 1);
}

#[tokio::test]
async fn refreshes_a_stale_token() {
    let server = AuthServer::start(Fixture::jwt_response("token-a", 1)).await;
    let cache = Fixture::cache(&server.url());

    assert_eq!(cache.token().await.unwrap(), "token-a");
    server.set_body(Fixture::jwt_response("token-b", 3600));
    assert_eq!(cache.token().await.unwrap(), "token-b");
    assert_eq!(server.requests(), 2);
}

#[tokio::test]
async fn falls_back_to_a_stale_token_when_refresh_fails() {
    let server = AuthServer::start(Fixture::jwt_response("token-a", 3)).await;
    let cache = Fixture::cache(&server.url());

    assert_eq!(cache.token().await.unwrap(), "token-a");
    drop(server);
    tokio::time::sleep(Duration::from_millis(1100)).await;

    assert_eq!(cache.token().await.unwrap(), "token-a");
}

#[tokio::test]
async fn concurrent_callers_share_a_single_refresh() {
    let server = AuthServer::start_with_delay(
        Fixture::jwt_response("token-a", 3600),
        Duration::from_millis(100),
    )
    .await;
    let cache = Arc::new(Fixture::cache(&server.url()));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move { cache.token().await.unwrap() }));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), "token-a");
    }
    assert_eq!(server.requests(), 1);
}
