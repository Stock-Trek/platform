use crate::api::ApiRequest;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use crate::limits::{LimitService, QUOTA_STREAM_CONNECTIONS, QUOTA_STREAM_SUBSCRIPTIONS};
use crate::streaming::StreamService;
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub(crate) struct LimitsApi {
    limits: Arc<LimitService>,
    sessions: Arc<SessionService>,
    streams: Arc<StreamService>,
}

impl LimitsApi {
    pub(crate) fn new(
        limits: Arc<LimitService>,
        sessions: Arc<SessionService>,
        streams: Arc<StreamService>,
    ) -> Self {
        Self {
            limits,
            sessions,
            streams,
        }
    }

    pub(crate) fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["limits"] => match *request.method() {
                Method::GET => self.limits(request),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn limits(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let owner = auth.owner();
        let mut effective = self.limits.effective(owner, Utc::now());
        if let Some(entry) = effective.get_mut(QUOTA_STREAM_CONNECTIONS) {
            entry.used = self.streams.connection_count(owner);
        }
        if let Some(entry) = effective.get_mut(QUOTA_STREAM_SUBSCRIPTIONS) {
            entry.used = self.streams.subscription_count(owner);
        }
        Ok(Respond::json(StatusCode::OK, &effective))
    }
}
