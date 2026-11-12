use super::support_tests::{AuthServiceBuilder, OWNER};
use crate::{
    ApiToken, JwtServer, OwnerIndex, TokenAuditEvent, TokenMutation, TokenRotationTransaction,
    TokenStore, jwt_server_error::JwtServerError,
};
use async_trait::async_trait;
use platform_errors::{AuthError, BpError, BpResult};
use platform_sigv4::{SigV4Config, SigV4Registry};
use reqwest::StatusCode;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::task::JoinHandle;

const ACCESS_KEY: &str = "AKIDRUNNER";
const STORE_ERROR: &str = "injected store failure";

struct FailingStore {
    retryable: bool,
}

impl FailingStore {
    fn new(retryable: bool) -> Self {
        Self { retryable }
    }

    fn error(&self) -> BpError {
        BpError::auth_store("read", "owner-index", self.retryable, STORE_ERROR)
    }
}

#[async_trait]
impl TokenStore for FailingStore {
    async fn put(&self, _token: &ApiToken) -> BpResult<()> {
        Err(self.error())
    }

    async fn get(&self, _id: &str) -> BpResult<Option<ApiToken>> {
        Err(self.error())
    }

    async fn delete(&self, _id: &str) -> BpResult<()> {
        Err(self.error())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        Err(self.error())
    }

    async fn owner_index(&self, _owner: &str) -> BpResult<Option<OwnerIndex>> {
        Err(self.error())
    }

    async fn update(&self, _expected: &ApiToken, _token: &ApiToken) -> BpResult<()> {
        Err(self.error())
    }

    async fn commit(&self, _mutation: TokenMutation, _event: TokenAuditEvent) -> BpResult<bool> {
        Err(self.error())
    }

    async fn commit_rotation(&self, _transaction: &TokenRotationTransaction) -> BpResult<()> {
        Err(self.error())
    }
}

struct Fixture;

impl Fixture {
    fn sigv4() -> SigV4Config {
        let registry = SigV4Registry::from_parts(ACCESS_KEY, "runner-secret", OWNER, None).unwrap();
        SigV4Config::new(registry, "eu-west-1", "execute-api").unwrap()
    }

    async fn server(retryable: bool) -> (String, JoinHandle<BpResult<()>>) {
        let service = AuthServiceBuilder::new()
            .store(Arc::new(FailingStore::new(retryable)))
            .build();
        let server =
            JwtServer::bind_with_sigv4("127.0.0.1:0".parse().unwrap(), service, Self::sigv4())
                .await
                .unwrap();
        let address = server.local_addr().unwrap();
        let handle = tokio::spawn(server.serve());
        (format!("http://{address}/jwt"), handle)
    }

    fn body() -> Vec<u8> {
        serde_json::to_vec(&json!({ "owner": OWNER })).unwrap()
    }

    fn signed_headers(url: &str, body: &[u8]) -> BTreeMap<String, String> {
        let mut headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        Self::sigv4()
            .signer_for_owner(OWNER)
            .unwrap()
            .sign("POST", url, &mut headers, body, SystemTime::now())
            .unwrap();
        headers
    }

    async fn exchange(retryable: bool) -> StatusCode {
        let (url, server) = Self::server(retryable).await;
        let body = Self::body();
        let mut request = reqwest::Client::new().post(&url).body(body.clone());
        for (name, value) in Self::signed_headers(&url, &body) {
            request = request.header(name, value);
        }
        let status = request.send().await.unwrap().status();
        server.abort();
        status
    }
}

#[tokio::test]
async fn retryable_issuance_failures_return_service_unavailable() {
    assert_eq!(
        Fixture::exchange(true).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn permanent_issuance_failures_return_bad_request() {
    assert_eq!(Fixture::exchange(false).await, StatusCode::BAD_REQUEST);
}

#[test]
fn unavailable_issuance_errors_include_retry_after() {
    let error = JwtServerError::issuance(BpError::Auth(AuthError::Unavailable {
        retry_after: Duration::from_secs(7),
    }));

    let response = error.response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(hyper::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("7")
    );
}
