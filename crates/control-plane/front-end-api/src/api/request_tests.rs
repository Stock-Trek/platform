use super::ApiRequest;
use crate::api::router::FrontEndApi;
use hyper::{HeaderMap, Method, Uri, body::Bytes};

fn request(path: &str) -> ApiRequest {
    let uri: Uri = path.parse().expect("uri");
    ApiRequest::from_parts(
        Method::GET,
        uri,
        HeaderMap::new(),
        Bytes::new(),
        None,
        hyper::http::Extensions::new(),
    )
}

fn header(name: &str, value: &str) -> HeaderMap {
    let mut map = HeaderMap::new();
    map.insert(
        hyper::header::HeaderName::from_bytes(name.as_bytes()).expect("name"),
        hyper::header::HeaderValue::from_str(value).expect("value"),
    );
    map
}

#[test]
fn query_values_are_percent_decoded() {
    let parsed = request("/v1/runs?from=2026-07-13T00%3A00%3A00.000Z&state=pending");
    assert_eq!(parsed.query("from"), Some("2026-07-13T00:00:00.000Z"));
    assert_eq!(parsed.query("state"), Some("pending"));
}

#[test]
fn malformed_escapes_stay_verbatim() {
    let parsed = request("/v1/algorithms/alg_1/builds?ref=heads%2Fmain&odd=100%");
    assert_eq!(parsed.query("ref"), Some("heads/main"));
    assert_eq!(parsed.query("odd"), Some("100%"));
}

#[test]
fn the_client_request_id_is_reused_when_valid() {
    let headers = header("x-request-id", "req_client_1");
    assert_eq!(
        FrontEndApi::header_request_id(&headers).as_deref(),
        Some("req_client_1")
    );
}

#[test]
fn a_blank_or_unusable_request_id_is_ignored() {
    assert_eq!(
        FrontEndApi::header_request_id(&header("x-request-id", "")),
        None
    );
    assert_eq!(
        FrontEndApi::header_request_id(&header("x-request-id", "nön-äscii")),
        None
    );
    assert_eq!(FrontEndApi::header_request_id(&HeaderMap::new()), None);
}
