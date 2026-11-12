use crate::JwtServer;
use crate::tests::support_tests::{OWNER, TestFixture};
use platform_sigv4::{SigV4Config, SigV4Registry};
use reqwest::StatusCode;
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration, time::SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
    time::timeout,
};

const ACCEPTED_ACCESS_KEY: &str = "AKIDRUNNER";
const OTHER_OWNER: &str = "other-owner";

struct Fixture;

impl Fixture {
    fn sigv4() -> SigV4Config {
        let registry =
            SigV4Registry::from_parts(ACCEPTED_ACCESS_KEY, "runner-secret", OWNER, None).unwrap();
        SigV4Config::new(registry, "eu-west-1", "execute-api").unwrap()
    }

    async fn server() -> (String, JoinHandle<platform_errors::BuildResult<()>>) {
        let service = TestFixture::service();
        TestFixture::add_token(&service, "token-a", OWNER).await;
        let server =
            JwtServer::bind_with_sigv4("127.0.0.1:0".parse().unwrap(), service, Self::sigv4())
                .await
                .unwrap();
        let address = server.local_addr().unwrap();
        let handle = tokio::spawn(server.serve());
        (format!("http://{address}/jwt"), handle)
    }

    fn body(owner: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({ "owner": owner })).unwrap()
    }

    fn signed_headers(url: &str, body: &[u8]) -> BTreeMap<String, String> {
        let mut headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        Fixture::sigv4()
            .signer_for_owner(OWNER)
            .unwrap()
            .sign("POST", url, &mut headers, body, SystemTime::now())
            .unwrap();
        headers
    }

    async fn send(
        url: &str,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
    ) -> (StatusCode, Option<Value>) {
        let mut request = reqwest::Client::new().post(url).body(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.unwrap();
        let status = response.status();
        let value = response.json::<Value>().await.ok();
        (status, value)
    }
    async fn raw_exchange(address: &str, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(response).unwrap()
    }
}

#[tokio::test]
async fn issues_a_jwt_to_a_signed_owner() {
    let (url, server) = Fixture::server().await;
    let body = Fixture::body(OWNER);
    let headers = Fixture::signed_headers(&url, &body);

    let (status, response) = Fixture::send(&url, headers, body).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        response
            .and_then(|value| value["token"].as_str().map(str::to_string))
            .is_some_and(|token| !token.is_empty())
    );
    server.abort();
}

#[tokio::test]
async fn rejects_a_missing_signature() {
    let (url, server) = Fixture::server().await;

    let (status, _) = Fixture::send(
        &url,
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        Fixture::body(OWNER),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    server.abort();
}

#[tokio::test]
async fn rejects_a_tampered_signature() {
    let (url, server) = Fixture::server().await;
    let body = Fixture::body(OWNER);
    let mut headers = Fixture::signed_headers(&url, &body);
    let authorization = headers.get("authorization").unwrap().to_string();
    let replacement = if authorization.ends_with('0') {
        '1'
    } else {
        '0'
    };
    let tampered = format!("{}{replacement}", &authorization[..authorization.len() - 1]);
    headers.insert("authorization".to_string(), tampered);

    let (status, _) = Fixture::send(&url, headers, body).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    server.abort();
}

#[tokio::test]
async fn rejects_an_owner_that_does_not_match_the_caller() {
    let (url, server) = Fixture::server().await;
    let body = Fixture::body(OTHER_OWNER);
    let headers = Fixture::signed_headers(&url, &body);

    let (status, _) = Fixture::send(&url, headers, body).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    server.abort();
}

#[tokio::test]
async fn rejects_an_oversized_declared_body_before_reading_it() {
    let (url, server) = Fixture::server().await;
    let address = url
        .strip_prefix("http://")
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    let request = format!(
        "POST /jwt HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        64 * 1024 + 1
    );

    let response = Fixture::raw_exchange(address, request.as_bytes()).await;

    assert!(response.starts_with("HTTP/1.1 413"));
    server.abort();
}
