use crate::api::Problem;
use http_body_util::Full;
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{CONTENT_TYPE, ETAG},
};
use serde::Serialize;

pub(crate) struct Respond;

impl Respond {
    pub(crate) fn json<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
        match serde_json::to_vec(value) {
            Ok(body) => Self::body(status, Bytes::from(body)),
            Err(error) => Problem::internal(error.to_string()).response(""),
        }
    }

    pub(crate) fn json_with_etag<T: Serialize>(
        status: StatusCode,
        value: &T,
        version: u64,
    ) -> Response<Full<Bytes>> {
        let mut response = Self::json(status, value);
        response
            .headers_mut()
            .insert(ETAG, format!("\"{version}\"").parse().expect("etag"));
        response
    }

    pub(crate) fn empty(status: StatusCode) -> Response<Full<Bytes>> {
        Self::body(status, Bytes::new())
    }

    fn body(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(body))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
    }
}
