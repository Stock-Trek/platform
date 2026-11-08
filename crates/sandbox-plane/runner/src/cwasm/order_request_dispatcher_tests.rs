use chrono::{Duration, Utc};
use platform_auth_service::{
    ApiToken, AuthService, AuthServiceConfig, MemoryTokenAuditLog, MemoryTokenStore,
    SignedOrderRequest, TokenExpiry, TokenOptions, TokenScope,
};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
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

const TOKEN_ID: &str = "runner-token";
const OWNER: &str = "runner-owner";

struct Fixture;

impl Fixture {
    fn auth_service() -> AuthService {
        AuthService::new(
            Arc::new(MemoryTokenStore::default()),
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
                audit: Arc::new(MemoryTokenAuditLog::new(4096)),
            },
        )
        .unwrap()
    }

    async fn setup(url: &str) -> (OrderRequestDispatcher, AuthService, ApiToken) {
        let auth_service = Self::auth_service();
        let options =
            TokenOptions::new(TokenExpiry::After(Duration::days(30)), TokenScope::DEFAULT);
        let now = Utc::now();
        let token = auth_service
            .add_token(TOKEN_ID, OWNER, now, options, now)
            .await
            .unwrap();
        let dispatcher = OrderRequestDispatcher::new(url, OWNER, auth_service.clone()).unwrap();
        (dispatcher, auth_service, token)
    }

    fn dispatcher_without_token(url: &str) -> OrderRequestDispatcher {
        OrderRequestDispatcher::new(url, OWNER, Self::auth_service()).unwrap()
    }

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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = Self::read(&mut stream).await;
            let response =
                format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });
        (format!("http://{address}/order-requests"), server)
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
    let result = OrderRequestDispatcher::new("not a url", OWNER, Fixture::auth_service());
    assert!(result.is_err());
}

#[test]
fn rejects_a_non_http_order_request_url() {
    let result = OrderRequestDispatcher::new("file:///tmp/orders", OWNER, Fixture::auth_service());
    assert!(result.is_err());
}

#[test]
fn rejects_an_empty_owner() {
    let result =
        OrderRequestDispatcher::new("http://127.0.0.1:1/orders", "", Fixture::auth_service());
    assert!(result.is_err());
}

#[tokio::test]
async fn signs_and_submits_place_order_actions() {
    let (url, server) = TestServer::start("200 OK").await;
    let (dispatcher, auth_service, token) = Fixture::setup(&url).await;

    dispatcher.dispatch([Fixture::place_order()]).await.unwrap();

    let request = server.await.unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/order-requests");
    assert_eq!(
        request.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );

    let body: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(body["cex"], "binance");
    assert_eq!(body["account"], "account-1");
    assert_eq!(body["tokenId"], token.id());

    let order_request: OrderRequest<AssetId, f64> =
        serde_json::from_value(body["orderRequest"].clone()).unwrap();
    let bytes = serde_json::to_vec(&order_request).unwrap();
    let signed = SignedOrderRequest::decode(body["signature"].as_str().unwrap()).unwrap();
    assert_eq!(signed.token_id(), token.id());
    assert!(
        auth_service
            .verify_order_request(token.id(), &bytes, signed.envelope(), Utc::now())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn rejects_an_owner_without_a_token() {
    let dispatcher = Fixture::dispatcher_without_token("http://127.0.0.1:1/order-requests");

    assert!(dispatcher.dispatch([Fixture::place_order()]).await.is_err());
}

#[tokio::test]
async fn ignores_actions_that_do_not_place_orders() {
    let dispatcher = Fixture::dispatcher_without_token("http://127.0.0.1:1/order-requests");
    let action = serde_json::to_string(&ResolvedAction::CancelAllOrders { tag: None }).unwrap();

    dispatcher.dispatch([action]).await.unwrap();
}

#[tokio::test]
async fn rejects_malformed_actions() {
    let dispatcher = Fixture::dispatcher_without_token("http://127.0.0.1:1/order-requests");

    assert!(dispatcher.dispatch(["{".to_string()]).await.is_err());
}

#[tokio::test]
async fn reports_a_rejected_submission() {
    let (url, server) = TestServer::start("503 Service Unavailable").await;
    let (dispatcher, _, _) = Fixture::setup(&url).await;

    assert!(dispatcher.dispatch([Fixture::place_order()]).await.is_err());
    server.await.unwrap();
}
