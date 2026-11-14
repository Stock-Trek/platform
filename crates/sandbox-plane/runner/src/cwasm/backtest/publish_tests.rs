use crate::{SimulationParameters, dto::StrategyContextDto};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_websockets::ServerBuilder;

use crate::cwasm::backtest::{publish::ResultPublisher, spec::Spec};

struct TestHttpServer;

impl TestHttpServer {
    async fn start(status: &str) -> (String, JoinHandle<String>) {
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
        (format!("http://{address}/results"), server)
    }

    async fn read(stream: &mut TcpStream) -> String {
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
        let content_length = head
            .split("\r\n")
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<usize>().ok())
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

struct TestWebSocketServer;

impl TestWebSocketServer {
    async fn start() -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (connection, _) = listener.accept().await.unwrap();
            let (_, mut stream) = ServerBuilder::new().accept(connection).await.unwrap();
            let message = stream.next().await.unwrap().unwrap();
            message.as_text().unwrap().to_string()
        });
        (format!("ws://{address}/results"), server)
    }
}

struct Fixture;

impl Fixture {
    fn spec(stream_url: &str, post_url: &str) -> Spec {
        Spec {
            strategy_name: "test".to_string(),
            strategy_context: StrategyContextDto::default(),
            initial_balances: BTreeMap::new(),
            market_slots: Vec::new(),
            driving_window_number: 0,
            windows: Vec::new(),
            historical_candle_count: 0,
            open_candle_progress_millis: 0,
            simulation_count: 1,
            max_concurrent_simulations: 1,
            random_seed: 0,
            simulation: SimulationParameters::default(),
            user_id: "user".to_string(),
            algorithm_id: "algorithm".to_string(),
            back_test_id: "back-test".to_string(),
            results_stream_url: stream_url.to_string(),
            results_post_url: post_url.to_string(),
            steps: 0,
        }
    }

    fn result() -> Value {
        json!({"status": "complete", "balance": 1000.0})
    }
}

#[tokio::test]
async fn publishes_the_result_to_both_results_endpoints() {
    let (stream_url, stream_server) = TestWebSocketServer::start().await;
    let (post_url, post_server) = TestHttpServer::start("200 OK").await;
    let result = Fixture::result();

    ResultPublisher::publish(&Fixture::spec(&stream_url, &post_url), &result)
        .await
        .unwrap();

    let streamed: Value = serde_json::from_str(&stream_server.await.unwrap()).unwrap();
    let posted: Value = serde_json::from_str(&post_server.await.unwrap()).unwrap();
    assert_eq!(streamed, result);
    assert_eq!(posted, result);
}

#[tokio::test]
async fn reports_failures_from_both_results_endpoints() {
    let error =
        ResultPublisher::publish(&Fixture::spec("not a url", "not a url"), &Fixture::result())
            .await
            .unwrap_err();

    assert!(error.to_string().contains("results stream failed"));
    assert!(error.to_string().contains("results post failed"));
}

#[tokio::test]
async fn reports_a_rejected_post_even_when_the_stream_succeeds() {
    let (stream_url, stream_server) = TestWebSocketServer::start().await;
    let (post_url, post_server) = TestHttpServer::start("503 Service Unavailable").await;

    let error =
        ResultPublisher::publish(&Fixture::spec(&stream_url, &post_url), &Fixture::result())
            .await
            .unwrap_err();

    stream_server.await.unwrap();
    post_server.await.unwrap();
    assert!(error.to_string().contains("503"));
}

#[tokio::test]
async fn posts_the_result_even_when_the_stream_fails() {
    let (post_url, post_server) = TestHttpServer::start("200 OK").await;
    let result = Fixture::result();

    ResultPublisher::publish(&Fixture::spec("not a url", &post_url), &result)
        .await
        .unwrap_err();

    let posted: Value = serde_json::from_str(&post_server.await.unwrap()).unwrap();
    assert_eq!(posted, result);
}

struct StalledServer;

impl StalledServer {
    async fn start(scheme: &str) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_connection, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        (format!("{scheme}://{address}/results"), server)
    }
}

#[tokio::test(start_paused = true)]
async fn times_out_a_stalled_stream_endpoint() {
    let (stream_url, server) = StalledServer::start("ws").await;

    let error =
        ResultPublisher::publish(&Fixture::spec(&stream_url, "not a url"), &Fixture::result())
            .await
            .unwrap_err();

    server.abort();
    assert!(error.to_string().contains("connection timed out"));
}

#[tokio::test(start_paused = true)]
async fn times_out_a_stalled_post_endpoint() {
    let (post_url, server) = StalledServer::start("http").await;

    let error =
        ResultPublisher::publish(&Fixture::spec("not a url", &post_url), &Fixture::result())
            .await
            .unwrap_err();

    server.abort();
    assert!(error.to_string().contains("timed out"), "{error}");
}
