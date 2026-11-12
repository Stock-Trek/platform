use platform_messages::UnsignedOrderRequest;
use platform_sigv4::{SigV4Config, SigV4Registry};
use serde_json::Value;
use std::collections::BTreeMap;
use stock_trek::{
    actions::ResolvedAction,
    types::{AccountId, Activation, AssetId, CexId, OrderRequest, Tag},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use super::OrderRequestDispatcher;

const OWNER: &str = "runner-owner";
const DEAD_AUTH: &str = "http://127.0.0.1:1/jwt";

struct Fixture;

impl Fixture {
    fn order_request() -> OrderRequest<AssetId, f64> {
        OrderRequest::MarketBuy {
            base: AssetId::Bitcoin,
            quote: AssetId::TetherUSD,
            activation: Activation::Immediate,
            quote_quantity: 250.0,
            tag: Tag::new("strategy-entry"),
        }
    }

    fn place_order() -> String {
        serde_json::to_string(&ResolvedAction::PlaceOrder {
            cex_id: CexId::Binance,
            account_id: AccountId("account-1".to_string()),
            order_request: Self::order_request(),
        })
        .unwrap()
    }

    fn sigv4() -> SigV4Config {
        let registry = SigV4Registry::from_parts("AKID", "secret", OWNER, None).unwrap();
        SigV4Config::new(registry, "eu-west-1", "execute-api").unwrap()
    }

    fn dispatcher(url: &str) -> OrderRequestDispatcher {
        Self::authenticated_dispatcher(url, DEAD_AUTH)
    }

    fn authenticated_dispatcher(url: &str, auth_service_url: &str) -> OrderRequestDispatcher {
        OrderRequestDispatcher::with_sigv4(url, auth_service_url, OWNER, &Self::sigv4()).unwrap()
    }

    fn jwt_response() -> String {
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        format!(r#"{{"token":"test-jwt","expiresAtUnix":{expires_at}}}"#)
    }
}

struct CapturedRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}

struct TestServer;

impl TestServer {
    async fn start(status: &str) -> (String, JoinHandle<CapturedRequest>) {
        Self::start_at("/order-requests", status, "").await
    }

    async fn start_at(
        path: &str,
        status: &str,
        body: &str,
    ) -> (String, JoinHandle<CapturedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let body = body.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = Self::read(&mut stream).await;
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });
        (format!("http://{address}{path}"), server)
    }

    async fn read(stream: &mut TcpStream) -> CapturedRequest {
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut chunk = [0; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let head = std::str::from_utf8(&bytes[..header_end]).unwrap();
        let mut lines = head.split("\r\n");
        let mut request_line = lines.next().unwrap().split_whitespace();
        let method = request_line.next().unwrap().to_string();
        let path = request_line.next().unwrap().to_string();
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
            .collect::<BTreeMap<_, _>>();
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or_default();
        while bytes.len() < header_end + content_length {
            let mut chunk = [0; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&chunk[..read]);
        }
        let body =
            String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap();
        CapturedRequest {
            method,
            path,
            headers,
            body,
        }
    }
}

#[test]
fn rejects_an_invalid_order_request_url() {
    assert!(
        OrderRequestDispatcher::with_sigv4("not a url", DEAD_AUTH, OWNER, &Fixture::sigv4())
            .is_err()
    );
}

#[test]
fn rejects_a_non_http_order_request_url() {
    assert!(
        OrderRequestDispatcher::with_sigv4(
            "file:///tmp/orders",
            DEAD_AUTH,
            OWNER,
            &Fixture::sigv4()
        )
        .is_err()
    );
}

#[test]
fn rejects_an_invalid_auth_service_url() {
    assert!(
        OrderRequestDispatcher::with_sigv4(
            "http://127.0.0.1:1/orders",
            "file:///tmp/jwt",
            OWNER,
            &Fixture::sigv4(),
        )
        .is_err()
    );
}

#[test]
fn rejects_an_empty_owner() {
    assert!(
        OrderRequestDispatcher::with_sigv4(
            "http://127.0.0.1:1/orders",
            DEAD_AUTH,
            "",
            &Fixture::sigv4()
        )
        .is_err()
    );
}

#[tokio::test]
async fn submits_unsigned_place_order_actions() {
    let (url, server) = TestServer::start("200 OK").await;
    let (auth_url, auth_server) =
        TestServer::start_at("/jwt", "200 OK", &Fixture::jwt_response()).await;
    let dispatcher = Fixture::authenticated_dispatcher(&url, &auth_url);

    dispatcher.dispatch([Fixture::place_order()]).await.unwrap();

    let auth_request = auth_server.await.unwrap();
    assert_eq!(auth_request.method, "POST");
    assert_eq!(auth_request.path, "/jwt");
    let body: Value = serde_json::from_str(&auth_request.body).unwrap();
    assert_eq!(body["owner"], OWNER);
    assert!(
        auth_request
            .headers
            .get("authorization")
            .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256 Credential=AKID/"))
    );
    assert!(auth_request.headers.contains_key("x-amz-date"));
    assert!(auth_request.headers.contains_key("host"));
    assert_eq!(
        auth_request.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    let request = server.await.unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/order-requests");
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Bearer test-jwt")
    );
    assert_eq!(
        request.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );

    let body: Value = serde_json::from_str(&request.body).unwrap();
    let payload: UnsignedOrderRequest = serde_json::from_value(body.clone()).unwrap();
    assert_eq!(payload.cex(), CexId::Binance);
    assert_eq!(payload.account(), &AccountId("account-1".to_string()));
    assert_eq!(payload.owner(), OWNER);
    assert_eq!(
        serde_json::to_value(payload.order_request()).unwrap(),
        serde_json::to_value(Fixture::order_request()).unwrap()
    );
    assert!(body.get("tokenId").is_none());
    assert!(body.get("signature").is_none());
}

#[tokio::test]
async fn normalizes_the_owner_before_sending_order_requests() {
    let (url, server) = TestServer::start("200 OK").await;
    let (auth_url, auth_server) =
        TestServer::start_at("/jwt", "200 OK", &Fixture::jwt_response()).await;
    let dispatcher = OrderRequestDispatcher::with_sigv4(
        &url,
        &auth_url,
        format!("  {OWNER}  "),
        &Fixture::sigv4(),
    )
    .unwrap();

    dispatcher.dispatch([Fixture::place_order()]).await.unwrap();

    let auth_request = auth_server.await.unwrap();
    let auth_body: Value = serde_json::from_str(&auth_request.body).unwrap();
    assert_eq!(auth_body["owner"], OWNER);
    let request = server.await.unwrap();
    let body: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(body["owner"], OWNER);
}

#[tokio::test]
async fn ignores_actions_that_do_not_place_orders() {
    let dispatcher = Fixture::dispatcher("http://127.0.0.1:1/order-requests");
    let action = serde_json::to_string(&ResolvedAction::CancelAllOrders { tag: None }).unwrap();

    dispatcher.dispatch([action]).await.unwrap();
}

#[tokio::test]
async fn rejects_malformed_actions() {
    let dispatcher = Fixture::dispatcher("http://127.0.0.1:1/order-requests");

    assert!(dispatcher.dispatch(["{".to_string()]).await.is_err());
}

#[tokio::test]
async fn reports_a_rejected_submission() {
    let (url, server) = TestServer::start("503 Service Unavailable").await;
    let (auth_url, auth_server) =
        TestServer::start_at("/jwt", "200 OK", &Fixture::jwt_response()).await;
    let dispatcher = Fixture::authenticated_dispatcher(&url, &auth_url);

    assert!(dispatcher.dispatch([Fixture::place_order()]).await.is_err());
    auth_server.await.unwrap();
    server.await.unwrap();
}
