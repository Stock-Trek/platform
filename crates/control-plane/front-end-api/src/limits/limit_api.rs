use crate::api::ApiRequest;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use crate::limits::LimitService;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub struct LimitsApi {
    limits: Arc<LimitService>,
    sessions: Arc<SessionService>,
}

impl LimitsApi {
    pub fn new(limits: Arc<LimitService>, sessions: Arc<SessionService>) -> Self {
        Self { limits, sessions }
    }

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
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
        let effective = self.limits.effective(auth.owner());
        Ok(Respond::json(StatusCode::OK, &effective))
    }
}
