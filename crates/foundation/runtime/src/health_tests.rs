use crate::{HealthConfig, HealthServer, HealthState};
use hyper::Method;
use platform_config::EnvVars;
use platform_errors::PlatformResult;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
};

struct TestRequest;

impl TestRequest {
    async fn send(address: SocketAddr, method: Method, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(address).await.expect("connect");
        let request = format!(
            "{} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            method.as_str()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        let response = String::from_utf8(response).expect("utf-8 response");
        let status = response
            .split_whitespace()
            .nth(1)
            .expect("status code")
            .parse()
            .expect("numeric status");
        (status, response)
    }
}

#[test]
fn health_config_reads_the_shared_address() {
    let vars = EnvVars::from_map([("HEALTH_BIND_ADDRESS", "127.0.0.1:8081")]);
    let config = HealthConfig::from_vars(&vars).expect("config");
    assert_eq!(
        config.address(),
        "127.0.0.1:8081".parse::<SocketAddr>().expect("address")
    );
}

#[test]
fn health_config_rejects_the_retired_address_alias() {
    let vars = EnvVars::from_map([("HEALTH_BIND_ADDR", "127.0.0.1:8081")]);
    assert!(HealthConfig::from_vars(&vars).is_err());
}

#[test]
fn health_config_rejects_the_retired_service_address_alias() {
    let vars = EnvVars::from_map([("AGGREGATOR_HEALTH_BIND_ADDR", "127.0.0.1:8082")]);
    assert!(HealthConfig::from_service_vars(&vars, "AGGREGATOR").is_err());
}

#[test]
fn health_config_prefers_the_service_address() {
    let vars = EnvVars::from_map([
        ("HEALTH_BIND_ADDRESS", "127.0.0.1:8081"),
        ("AGGREGATOR_HEALTH_BIND_ADDRESS", "127.0.0.1:8082"),
    ]);
    let config = HealthConfig::from_service_vars(&vars, "AGGREGATOR").expect("config");
    assert_eq!(
        config.address(),
        "127.0.0.1:8082".parse::<SocketAddr>().expect("address")
    );
}

#[test]
fn health_config_uses_the_shared_address_as_a_fallback() {
    let vars = EnvVars::from_map([("HEALTH_BIND_ADDRESS", "127.0.0.1:8081")]);
    let config = HealthConfig::from_service_vars(&vars, "AGGREGATOR").expect("config");
    assert_eq!(
        config.address(),
        "127.0.0.1:8081".parse::<SocketAddr>().expect("address")
    );
}

#[test]
fn health_config_rejects_an_invalid_address() {
    let vars = EnvVars::from_map([("HEALTH_BIND_ADDRESS", "not an address")]);
    assert!(HealthConfig::from_vars(&vars).is_err());
}

#[test]
fn health_config_rejects_a_missing_address() {
    let vars = EnvVars::from_map(Vec::<(String, String)>::new());
    assert!(HealthConfig::from_vars(&vars).is_err());
    assert!(HealthConfig::from_service_vars(&vars, "AGGREGATOR").is_err());
}

#[tokio::test]
async fn health_endpoints_reflect_readiness() {
    let state = HealthState::new();
    let server = HealthServer::bind("127.0.0.1:0".parse().expect("address"), state.clone())
        .await
        .expect("bind");
    let address = server.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<PlatformResult<()>>();
    let task = tokio::spawn(server.serve_with_shutdown(async move {
        match shutdown_rx.await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    }));

    assert_eq!(
        TestRequest::send(address, Method::GET, "/healthz").await.0,
        204
    );
    assert_eq!(
        TestRequest::send(address, Method::GET, "/readyz").await.0,
        503
    );

    state.mark_ready();

    assert_eq!(
        TestRequest::send(address, Method::GET, "/readyz").await.0,
        204
    );
    assert_eq!(
        TestRequest::send(address, Method::HEAD, "/healthz").await.0,
        204
    );
    assert_eq!(
        TestRequest::send(address, Method::GET, "/missing").await.0,
        404
    );

    let (status, response) = TestRequest::send(address, Method::POST, "/healthz").await;
    assert_eq!(status, 405);
    assert!(response.to_ascii_lowercase().contains("allow: get, head"));

    shutdown_tx.send(Ok(())).expect("shutdown");
    task.await.expect("task").expect("server");
}
