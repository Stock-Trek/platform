use super::transport::{
    AccountPositionEvent, AccountSnapshot, BinanceAccountEvent, BinanceEventStream,
    BinanceTransport,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::{SinkExt, StreamExt};
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    binance::{
        account_status::BinanceAccountStatusRequest,
        exchange::BinanceExchange,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    http::{HttpMethod, HttpRequest, HttpResponse},
    request::{ETHttpRequest, ETRequest},
    response::ETHttpResponse,
    ticker::Ticker,
};
use platform_type_bridge::{BinanceConverter, TickerMapping};
use platform_types::new_types::TimestampMilliseconds;
use reqwest::Client;
use serde::Deserialize;
use std::{collections::BTreeMap, time::Duration};
use tokio::net::TcpStream;
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message, WebSocketStream, proto::CloseCode};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ReqwestBinanceTransport {
    client: Client,
    rest_url: String,
    websocket_url: String,
    mapping: &'static TickerMapping,
}

impl ReqwestBinanceTransport {
    pub fn new(rest_url: impl Into<String>, websocket_url: impl Into<String>) -> BpResult<Self> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            client,
            rest_url: rest_url.into().trim_end_matches('/').to_string(),
            websocket_url: websocket_url.into().trim_end_matches('/').to_string(),
            mapping: BinanceConverter::ticker_mapping_static(),
        })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.rest_url, path.trim_start_matches('/'))
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        query: Option<&str>,
        headers: &[(String, String)],
    ) -> BpResult<HttpResponse> {
        let mut url = self.endpoint(path);
        if let Some(query) = query {
            url.push('?');
            url.push_str(query);
        }
        let mut request = self.client.request(method, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(BpError::external)?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let body = response.bytes().await.map_err(BpError::external)?.to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn listen_key_request(
        &self,
        method: reqwest::Method,
        credentials: &ApiKeyCredentials,
        query: Option<&str>,
    ) -> BpResult<()> {
        let response = self
            .send(
                method,
                "userDataStream",
                query,
                &[("X-MBX-APIKEY".to_string(), credentials.api_key.clone())],
            )
            .await?;
        Self::ensure_success(response).map(|_| ())
    }

    fn ensure_success(response: HttpResponse) -> BpResult<Vec<u8>> {
        if (200..300).contains(&response.status) {
            return Ok(response.body);
        }
        Err(Self::response_error(response))
    }

    fn response_error(response: HttpResponse) -> BpError {
        let body = String::from_utf8_lossy(&response.body);
        BpError::Exchange(ExchangeError::BadResponse(format!(
            "Binance returned HTTP {}: {body}",
            response.status
        )))
    }

    fn parse_listen_key(body: &[u8]) -> BpResult<String> {
        #[derive(Deserialize)]
        struct ListenKey {
            #[serde(rename = "listenKey")]
            listen_key: String,
        }
        let value: ListenKey = serde_json::from_slice(body).map_err(BpError::external)?;
        if value.listen_key.is_empty() {
            return Err(BpError::Exchange(ExchangeError::BadResponse(
                "empty listen key".to_string(),
            )));
        }
        Ok(value.listen_key)
    }

    fn parse_account_snapshot(&self, response: HttpResponse) -> BpResult<AccountSnapshot> {
        if !(200..300).contains(&response.status) {
            return Err(Self::response_error(response));
        }
        let response = BinanceResponse::<
            platform_exchange_types::binance::account_status::BinanceAccountStatusResponse,
        >::try_from_http(response)?;
        let account = match response.payload {
            BinanceResponsePayload::Success(account) => account,
            BinanceResponsePayload::Failure(error) => {
                return Err(BpError::Exchange(ExchangeError::BadResponse(
                    error.to_string(),
                )));
            }
        };
        let occurred_at = Utc
            .timestamp_millis_opt(account.updateTime)
            .single()
            .unwrap_or_else(Utc::now);
        let mut balances = BTreeMap::new();
        for balance in account.balances {
            let Some(asset) = self.mapping.asset_id(&Ticker(balance.asset)) else {
                continue;
            };
            let free = Self::decimal_to_f64(&balance.free)?;
            let locked = Self::decimal_to_f64(&balance.locked)?;
            balances.insert(*asset, free + locked);
        }
        Ok(AccountSnapshot::new(balances, occurred_at))
    }

    fn decimal_to_f64(value: &rust_decimal::Decimal) -> BpResult<f64> {
        let value = value
            .to_string()
            .parse::<f64>()
            .map_err(|error| BpError::Exchange(ExchangeError::BadResponse(error.to_string())))?;
        if !value.is_finite() {
            return Err(BpError::Exchange(ExchangeError::BadResponse(
                "non-finite balance".to_string(),
            )));
        }
        Ok(value)
    }
}

#[async_trait]
impl BinanceTransport for ReqwestBinanceTransport {
    async fn create_listen_key(&self, credentials: &ApiKeyCredentials) -> BpResult<String> {
        let response = self
            .send(
                reqwest::Method::POST,
                "userDataStream",
                None,
                &[("X-MBX-APIKEY".to_string(), credentials.api_key.clone())],
            )
            .await?;
        Self::parse_listen_key(&Self::ensure_success(response)?)
    }

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()> {
        self.listen_key_request(
            reqwest::Method::PUT,
            credentials,
            Some(&format!("listenKey={listen_key}")),
        )
        .await
    }

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()> {
        self.listen_key_request(
            reqwest::Method::DELETE,
            credentials,
            Some(&format!("listenKey={listen_key}")),
        )
        .await
    }

    async fn account_snapshot(&self, credentials: &ApiKeyCredentials) -> BpResult<AccountSnapshot> {
        let signer = BinanceExchange::signer_hmac_sha256(credentials.clone())?;
        let mut request = BinanceAccountStatusRequest::new();
        request.set_timestamp(TimestampMilliseconds::now());
        let request: HttpRequest = request.try_into_http(&signer)?;
        let response = self
            .send(
                Self::reqwest_method(request.method),
                &request.path,
                request.query.as_deref(),
                &request.headers,
            )
            .await?;
        self.parse_account_snapshot(response)
    }

    async fn open_stream(&self, listen_key: &str) -> BpResult<Box<dyn BinanceEventStream>> {
        let url = format!("{}/{}", self.websocket_url, listen_key);
        let builder = ClientBuilder::new().uri(&url).map_err(BpError::external)?;
        let (stream, _) = builder.connect().await.map_err(BpError::external)?;
        Ok(Box::new(BinanceWebsocketStream {
            stream,
            mapping: self.mapping,
        }))
    }
}

impl ReqwestBinanceTransport {
    fn reqwest_method(method: HttpMethod) -> reqwest::Method {
        match method {
            HttpMethod::GET => reqwest::Method::GET,
            HttpMethod::DELETE => reqwest::Method::DELETE,
            HttpMethod::PATCH => reqwest::Method::PATCH,
            HttpMethod::POST => reqwest::Method::POST,
            HttpMethod::PUT => reqwest::Method::PUT,
        }
    }
}

struct BinanceWebsocketStream {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    mapping: &'static TickerMapping,
}

#[async_trait]
impl BinanceEventStream for BinanceWebsocketStream {
    async fn next(&mut self) -> BpResult<Option<BinanceAccountEvent>> {
        loop {
            let message = self.stream.next().await.ok_or_else(|| {
                BpError::Exchange(ExchangeError::BadResponse(
                    "Binance websocket closed".to_string(),
                ))
            })?;
            let message = message.map_err(BpError::external)?;
            if message.is_ping() {
                self.stream
                    .send(Message::pong(message.into_payload()))
                    .await
                    .map_err(BpError::external)?;
                continue;
            }
            if message.is_close() {
                self.stream
                    .send(Message::close(Some(CloseCode::NORMAL_CLOSURE), ""))
                    .await
                    .ok();
                return Ok(None);
            }
            let Some(payload) = message.as_text() else {
                continue;
            };
            if let Some(event) = self.parse_payload(payload)? {
                return Ok(Some(event));
            }
        }
    }
}

impl BinanceWebsocketStream {
    fn parse_payload(&self, payload: &str) -> BpResult<Option<BinanceAccountEvent>> {
        let transport = EventParser {
            mapping: self.mapping,
        };
        transport.parse(payload)
    }
}

struct EventParser {
    mapping: &'static TickerMapping,
}

impl EventParser {
    fn parse(&self, payload: &str) -> BpResult<Option<BinanceAccountEvent>> {
        #[derive(Deserialize)]
        struct RawBalance {
            #[serde(rename = "a")]
            asset: String,
            #[serde(rename = "f")]
            free: String,
            #[serde(rename = "l")]
            locked: String,
        }
        #[derive(Deserialize)]
        struct RawEvent {
            #[serde(rename = "e")]
            kind: String,
            #[serde(rename = "E")]
            occurred_at: i64,
            #[serde(rename = "u")]
            version: Option<u64>,
            #[serde(rename = "B")]
            balances: Option<Vec<RawBalance>>,
        }
        let raw: RawEvent = serde_json::from_str(payload).map_err(BpError::external)?;
        if raw.kind == "listenKeyExpired" {
            return Ok(Some(BinanceAccountEvent::ListenKeyExpired));
        }
        if raw.kind != "outboundAccountPosition" {
            return Ok(None);
        }
        let Some(version) = raw.version else {
            return Ok(None);
        };
        let Some(raw_balances) = raw.balances else {
            return Ok(None);
        };
        let mut balances = BTreeMap::new();
        for balance in raw_balances {
            let Some(asset) = self.mapping.asset_id(&Ticker(balance.asset)) else {
                continue;
            };
            let free = balance.free.parse::<f64>().map_err(|error| {
                BpError::Exchange(ExchangeError::BadResponse(error.to_string()))
            })?;
            let locked = balance.locked.parse::<f64>().map_err(|error| {
                BpError::Exchange(ExchangeError::BadResponse(error.to_string()))
            })?;
            if !free.is_finite() || !locked.is_finite() {
                return Err(BpError::Exchange(ExchangeError::BadResponse(
                    "non-finite balance".to_string(),
                )));
            }
            balances.insert(*asset, free + locked);
        }
        let occurred_at = Utc
            .timestamp_millis_opt(raw.occurred_at)
            .single()
            .ok_or_else(|| {
                BpError::Exchange(ExchangeError::BadResponse(
                    "invalid event timestamp".to_string(),
                ))
            })?;
        Ok(Some(BinanceAccountEvent::Position(
            AccountPositionEvent::new(version, balances, occurred_at),
        )))
    }
}
