use crate::api::{Problem, Respond};
use http_body_util::Full;
use hyper::{Method, Response, StatusCode, body::Bytes};

const HEALTH_PATHS: [&str; 2] = ["/healthz", "/readyz"];
const ALLOWED_METHODS: &[Method] = &[Method::GET, Method::HEAD];

pub struct HealthRoute;

impl HealthRoute {
    pub fn matches(path: &str) -> bool {
        HEALTH_PATHS.contains(&path)
    }

    pub fn dispatch(method: &Method) -> Response<Full<Bytes>> {
        match *method {
            Method::GET | Method::HEAD => Respond::empty(StatusCode::NO_CONTENT),
            _ => Problem::method_not_allowed(ALLOWED_METHODS).response(""),
        }
    }
}
