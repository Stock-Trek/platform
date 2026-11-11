use chrono::{Duration, Utc};
use platform_auth_service::{
    ApiToken, AuthService, AuthServiceConfig, MemoryReplayStore, MemoryTokenStore, TokenMetadata,
    TokenScope, TokenStore,
};
use platform_messages::order_request::UnsignedOrderRequest;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
use stock_trek_types::cex::{
    account_id::AccountId, activation::Activation, asset_id::AssetId, cex_id::CexId,
    order_request::OrderRequest, tag::Tag,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

pub(super) const OWNER: &str = "portfolio-owner";

pub(super) struct Fixture;

impl Fixture {
    pub(super) fn order_request() -> OrderRequest<AssetId, f64> {
        OrderRequest::MarketBuy {
            base: AssetId::Bitcoin,
            quote: AssetId::TetherUSD,
            activation: Activation::Immediate,
            quote_quantity: 250.0,
            tag: Tag::new("strategy-entry"),
        }
    }

    pub(super) fn unsigned() -> UnsignedOrderRequest {
        UnsignedOrderRequest::new(
            CexId::Binance,
            AccountId("account-1".to_string()),
            OWNER,
            Self::order_request(),
        )
    }

    pub(super) async fn auth_service() -> (AuthService, ApiToken) {
        let store = Arc::new(MemoryTokenStore::default());
        let now = Utc::now();
        let metadata = TokenMetadata::new(now + Duration::days(30), TokenScope::DEFAULT).unwrap();
        let token = ApiToken::generate("portfolio-token", OWNER, now, metadata).unwrap();
        store.put(&token).await.unwrap();
        let service = AuthService::new(
            store,
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
            },
            Arc::new(MemoryReplayStore::default()),
        )
        .unwrap();
        (service, token)
    }

    pub(super) fn address() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    pub(super) async fn downstream() -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = Self::read_body(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            body
        });
        (format!("http://{address}/signed-orders"), task)
    }

    pub(super) async fn read_body(stream: &mut TcpStream) -> String {
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
        let headers = head
            .split("\r\n")
            .skip(1)
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
        String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap()
    }
}
