use crate::{
    binance::{exchange::BinanceExchange, response::BinanceResponse},
    encode::ByteEncoder,
    http::{HttpMethod, HttpRequest},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest},
    signer::Signer,
};
use platform_errors::{BuildError, BuildResult};
use platform_types::{TimestampMilliseconds, UsageCount};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinanceListenKeyAction {
    Create,
    Keepalive,
    Close,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceListenKeyRequest {
    #[serde(skip)]
    action: BinanceListenKeyAction,
    #[serde(skip)]
    listen_key: Option<String>,
}

impl BinanceListenKeyRequest {
    pub fn create() -> Self {
        Self {
            action: BinanceListenKeyAction::Create,
            listen_key: None,
        }
    }

    pub fn keepalive(listen_key: impl Into<String>) -> Self {
        Self {
            action: BinanceListenKeyAction::Keepalive,
            listen_key: Some(listen_key.into()),
        }
    }

    pub fn close(listen_key: impl Into<String>) -> Self {
        Self {
            action: BinanceListenKeyAction::Close,
            listen_key: Some(listen_key.into()),
        }
    }
}

impl ETRequest for BinanceListenKeyRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        false
    }

    fn is_idempotent(&self) -> bool {
        self.action != BinanceListenKeyAction::Create
    }

    fn rate_limit_usage(&self, _restriction: RateLimitRestriction) -> UsageCount {
        UsageCount::ZERO
    }

    fn set_api_key(&mut self, _api_key: Option<String>) {}

    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}

    fn query_params(&self, percent_encode: bool) -> String {
        let Some(listen_key) = self.listen_key.as_deref() else {
            return String::new();
        };
        if percent_encode {
            format!(
                "listenKey={}",
                ByteEncoder::Percent.encode(listen_key.as_bytes())
            )
        } else {
            format!("listenKey={listen_key}")
        }
    }
}

impl ETHttpRequest for BinanceListenKeyRequest {
    type Response = BinanceResponse<BinanceListenKeyResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "userDataStream"
    }

    fn http_method(&self) -> HttpMethod {
        match self.action {
            BinanceListenKeyAction::Create => HttpMethod::POST,
            BinanceListenKeyAction::Keepalive => HttpMethod::PUT,
            BinanceListenKeyAction::Close => HttpMethod::DELETE,
        }
    }

    fn try_into_http(self, signer: &Signer) -> BuildResult<HttpRequest> {
        let query = self.query_params(true);
        Ok(HttpRequest {
            method: self.http_method(),
            path: self.http_endpoint().to_string(),
            query: (!query.is_empty()).then_some(query),
            headers: vec![("X-MBX-APIKEY".to_string(), signer.api_key())],
            body: None,
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey", default)]
    listen_key: Option<String>,
}

impl BinanceListenKeyResponse {
    pub fn require_listen_key(self) -> BuildResult<String> {
        let listen_key = self
            .listen_key
            .filter(|listen_key| !listen_key.is_empty())
            .ok_or_else(|| BuildError::bad_value("listen key", "", "is missing"))?;
        Ok(listen_key)
    }
}
