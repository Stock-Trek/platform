use crate::api::{ApiRequest, Problem, Respond};
use crate::auth::{Scope, SessionService};
use crate::limits::{LimitService, QUOTA_STREAM_CONNECTIONS};
use crate::streaming::StreamService;
use crate::streaming::stream_types::StreamTicket;
use crate::streaming::ws;
use chrono::Utc;
use http_body_util::Full;
use hyper::{
    Method, Response, StatusCode,
    body::Bytes,
    header::{
        CONNECTION, HeaderValue, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION,
        UPGRADE,
    },
};
use hyper_util::rt::TokioIo;
use std::sync::Arc;

const STREAM_CONNECTION_RETRY_SECONDS: u64 = 1;
const WEBSOCKET_VERSION: &str = "13";

pub(crate) struct StreamApi {
    streams: Arc<StreamService>,
    limits: Arc<LimitService>,
    sessions: Arc<SessionService>,
}

impl StreamApi {
    pub(crate) fn new(
        streams: Arc<StreamService>,
        limits: Arc<LimitService>,
        sessions: Arc<SessionService>,
    ) -> Self {
        Self {
            streams,
            limits,
            sessions,
        }
    }

    pub(crate) fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["stream"] => match *request.method() {
                Method::GET => self.stream(request),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["stream", "ticket"] => match *request.method() {
                Method::POST => self.ticket(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn ticket(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let session_id = self
            .sessions
            .session_id(auth.token())
            .ok_or_else(|| Problem::unauthorized("the token is missing, expired, or revoked"))?;
        let (ticket, expires_at) = self
            .streams
            .issue_ticket(auth.owner(), &session_id, Utc::now());
        Ok(Respond::json(
            StatusCode::OK,
            &StreamTicket { ticket, expires_at },
        ))
    }

    fn stream(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let ticket = request
            .query("ticket")
            .ok_or_else(|| Problem::unauthorized("a stream ticket is required"))?;
        let (owner, session_id) =
            self.streams
                .redeem_ticket(ticket, Utc::now())
                .ok_or_else(|| {
                    Problem::unauthorized("the stream ticket is unknown, used, or expired")
                })?;
        let limit = self
            .limits
            .entry(&owner, QUOTA_STREAM_CONNECTIONS, Utc::now());
        if self.streams.connection_count(&owner) + 1 > limit.limit {
            return Err(Problem::rate_limited(STREAM_CONNECTION_RETRY_SECONDS));
        }
        let accept = Self::handshake(request)?;
        Self::upgrade(
            request,
            accept,
            self.streams.clone(),
            self.sessions.clone(),
            owner,
            session_id,
        )
    }

    fn handshake(request: &ApiRequest) -> Result<String, Problem> {
        let headers = request.headers();
        let websocket = headers
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
        let version = headers
            .get(SEC_WEBSOCKET_VERSION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == WEBSOCKET_VERSION);
        let key = headers
            .get(SEC_WEBSOCKET_KEY)
            .and_then(|value| value.to_str().ok());
        if !websocket || !version {
            return Err(Problem::validation_failed(
                "the request must be a version 13 WebSocket handshake",
            ));
        }
        let key = key.ok_or_else(|| {
            Problem::validation_failed("the WebSocket handshake needs a Sec-WebSocket-Key")
        })?;
        ws::Connection::accept_key(key)
            .ok_or_else(|| Problem::validation_failed("the Sec-WebSocket-Key is malformed"))
    }

    fn upgrade(
        request: &ApiRequest,
        accept: String,
        streams: Arc<StreamService>,
        sessions: Arc<SessionService>,
        owner: String,
        session_id: String,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let on_upgrade = request
            .extensions()
            .get::<hyper::upgrade::OnUpgrade>()
            .cloned()
            .ok_or_else(Problem::unavailable)?;
        let mut response = Response::new(Full::default());
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        let headers = response.headers_mut();
        headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
        headers.insert(
            SEC_WEBSOCKET_ACCEPT,
            HeaderValue::from_str(&accept).expect("accept header"),
        );
        tokio::spawn(async move {
            match on_upgrade.await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let ws = tokio_websockets::server::Builder::new().serve(io);
                    ws::Connection::serve(ws, streams, sessions, owner, session_id).await;
                }
                Err(error) => {
                    tracing::warn!(%error, "front-end api stream upgrade failed");
                }
            }
        });
        Ok(response)
    }
}
