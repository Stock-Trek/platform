use platform_messages::order_request::UnsignedOrderRequest;
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

    fn dispatcher(url: &str) -> OrderRequestDispatcher {
        OrderRequestDispatcher::new(url, OWNER).unwrap()
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
    assert!(OrderRequestDispatcher::new("not a url", OWNER).is_err());
}

#[test]
fn rejects_a_non_http_order_request_url() {
    assert!(OrderRequestDispatcher::new("file:///tmp/orders", OWNER).is_err());
}

#[test]
fn rejects_an_empty_owner() {
    assert!(OrderRequestDispatcher::new("http://127.0.0.1:1/orders", "").is_err());
}

#[tokio::test]
async fn submits_unsigned_place_order_actions() {
    let (url, server) = TestServer::start("200 OK").await;
    let dispatcher = Fixture::dispatcher(&url);

    dispatcher.dispatch([Fixture::place_order()]).await.unwrap();

    let request = server.await.unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/order-requests");
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
    let dispatcher = Fixture::dispatcher(&url);

    assert!(dispatcher.dispatch([Fixture::place_order()]).await.is_err());
    server.await.unwrap();
}
