use super::{AdminApi, ApiServer, ReadApi, RequestHandler};
use crate::{MemoryOwnershipStore, RegistryService};
use hyper::StatusCode;
use platform_auth_service::JwksCache;
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::Arc,
};
use tokio::sync::watch;

struct Fixture;

impl Fixture {
    fn admin() -> Arc<dyn RequestHandler> {
        let service = Arc::new(RegistryService::new(Arc::new(MemoryOwnershipStore::new())));
        Arc::new(AdminApi::new(service, "secret"))
    }

    fn read() -> Arc<dyn RequestHandler> {
        let service = Arc::new(RegistryService::new(Arc::new(MemoryOwnershipStore::new())));
        let jwks = JwksCache::from_jwks(serde_json::from_str(r#"{"keys":[]}"#).expect("jwks"));
        Arc::new(ReadApi::new(service, jwks))
    }

    async fn status_and_allow(
        handler: Arc<dyn RequestHandler>,
        method: &'static str,
        path: &'static str,
        token: Option<&'static str>,
    ) -> (u16, Option<String>) {
        let server = ApiServer::bind("127.0.0.1:0".parse().expect("address"), handler)
            .await
            .expect("bind");
        let address = server.local_addr().expect("address");
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let serving = tokio::spawn(async move {
            server
                .serve_with_shutdown(async move {
                    let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
                    Ok(())
                })
                .await
        });
        let response =
            tokio::task::spawn_blocking(move || Self::request(address, method, path, token))
                .await
                .expect("request");
        let _ = shutdown_tx.send(true);
        serving.await.expect("join").expect("serve");
        Self::parse(&response)
    }

    fn request(address: SocketAddr, method: &str, path: &str, token: Option<&str>) -> String {
        let mut stream = TcpStream::connect(address).expect("connect");
        let authorization = token
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\n{authorization}Connection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        response
    }

    fn parse(response: &str) -> (u16, Option<String>) {
        let mut lines = response.lines();
        let status = lines
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse().ok())
            .expect("status");
        let allow = lines.take_while(|line| !line.is_empty()).find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("allow")
                .then(|| value.trim().to_string())
        });
        (status, allow)
    }
}

#[tokio::test]
async fn admin_health_reports_allowed_methods() {
    let (status, allow) =
        Fixture::status_and_allow(Fixture::admin(), "POST", "/healthz", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED.as_u16());
    assert_eq!(allow.as_deref(), Some("GET"));
}

#[tokio::test]
async fn admin_ownership_reports_allowed_methods() {
    let (status, allow) =
        Fixture::status_and_allow(Fixture::admin(), "GET", "/v1/ownership", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED.as_u16());
    assert_eq!(allow.as_deref(), Some("POST, DELETE"));
}

#[tokio::test]
async fn read_accounts_reports_allowed_methods() {
    let (status, allow) =
        Fixture::status_and_allow(Fixture::read(), "POST", "/v1/accounts", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED.as_u16());
    assert_eq!(allow.as_deref(), Some("GET"));
}

#[tokio::test]
async fn read_health_reports_allowed_methods() {
    let (status, allow) =
        Fixture::status_and_allow(Fixture::read(), "DELETE", "/readyz", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED.as_u16());
    assert_eq!(allow.as_deref(), Some("GET"));
}
