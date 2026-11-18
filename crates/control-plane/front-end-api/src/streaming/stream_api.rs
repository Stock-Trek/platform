use crate::api::{ApiRequest, Problem, Respond};
use crate::auth::{Scope, SessionService};
use crate::limits::{LimitService, QUOTA_STREAM_CONNECTIONS};
use crate::streaming::StreamService;
use crate::streaming::stream_types::StreamTicket;
use chrono::Utc;
use http_body_util::Full;
use hyper::{Method, Response, StatusCode, body::Bytes};
use std::sync::Arc;

pub struct StreamApi {
    streams: Arc<StreamService>,
    limits: Arc<LimitService>,
    sessions: Arc<SessionService>,
}

impl StreamApi {
    pub fn new(
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

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
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
        let (ticket, expires_at) = self.streams.issue_ticket(auth.owner(), Utc::now());
        Ok(Respond::json(
            StatusCode::OK,
            &StreamTicket { ticket, expires_at },
        ))
    }

    fn stream(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let ticket = request
            .query("ticket")
            .ok_or_else(|| Problem::unauthorized("a stream ticket is required"))?;
        let owner = self
            .streams
            .redeem_ticket(ticket, Utc::now())
            .ok_or_else(|| {
                Problem::unauthorized("the stream ticket is unknown, used, or expired")
            })?;
        self.limits
            .check(&owner, QUOTA_STREAM_CONNECTIONS, 1)
            .map_err(|mut problem| {
                if problem.status() == StatusCode::FORBIDDEN {
                    problem = Problem::rate_limited(1);
                }
                problem
            })?;
        // TODO: upgrade the connection to WebSocket, then serve subscribe/unsubscribe
        // actions, replay and gap frames, and ping every 30 seconds over it.
        Err(Problem::unavailable())
    }
}
