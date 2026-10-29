use super::GitHubEventConverter;
use crate::{
    dto::event_converter::EventConverter, event_response::EventResponse,
};
use hmac::{Hmac, KeyInit, Mac};
use lambda_http::Body;
use platform_errors::{BpError, BpResult, GitHubError};
use platform_github::host_allow_list::HostAllowlist;
use sha2::Sha256;

const SECRET: &str = "top-secret";

fn converter() -> GitHubEventConverter {
    converter_with_hosts(["github.com", "ghe.example.com"])
}

fn converter_with_hosts<I, S>(hosts: I) -> GitHubEventConverter
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    GitHubEventConverter {
        secret: SECRET.to_owned(),
        allowed_hosts: HostAllowlist::from_hosts(hosts).unwrap(),
    }
}

fn host_from_converter(converter: &GitHubEventConverter, html_url: &str) -> BpResult<String> {
    let body = installation_payload(html_url);
    let request = signed_request(SECRET, "installation", &body);
    match converter.event_response(&request)? {
        EventResponse::Accept(messages) => Ok(messages[0].source.host().to_owned()),
        other => panic!("expected accept, got {other:?}"),
    }
}

fn signed_request(secret: &str, event_type: &str, body: &[u8]) -> lambda_http::Request {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    let signature = hex::encode(mac.finalize().into_bytes());
    http::Request::builder()
        .header("x-github-event", event_type)
        .header("x-hub-signature-256", format!("sha256={signature}"))
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn installation_payload(html_url: &str) -> Vec<u8> {
    format!(
        r#"{{"action":"created","installation":{{"id":123,"repository_selection":"all","html_url":{html_url:?}}}}}"#
    )
    .into_bytes()
}

fn host_from_installation(html_url: &str) -> BpResult<String> {
    host_from_converter(&converter(), html_url)
}

#[test]
fn verifies_valid_signature() {
    let body = br#"{"action":"ping"}"#;
    let request = signed_request(SECRET, "ping", body);
    assert!(GitHubEventConverter::verify_signature(&request, SECRET));
}

#[test]
fn rejects_missing_signature() {
    let request = http::Request::builder()
        .header("x-github-event", "ping")
        .body(Body::from(br#"{"action":"ping"}"#.to_vec()))
        .unwrap();
    assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
}

#[test]
fn rejects_wrong_signature() {
    let body = br#"{"action":"ping"}"#;
    let request = signed_request("not-the-secret", "ping", body);
    assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
}

#[test]
fn rejects_tampered_body() {
    let body = br#"{"action":"ping"}"#;
    let mut request = signed_request(SECRET, "ping", body);
    *request.body_mut() = Body::from(br#"{"action":"pong"}"#.to_vec());
    assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
}

#[test]
fn rejects_malformed_signature_header() {
    let body = br#"{"action":"ping"}"#;
    let request = http::Request::builder()
        .header("x-github-event", "ping")
        .header("x-hub-signature-256", "sha1=deadbeef")
        .body(Body::from(body.to_vec()))
        .unwrap();
    assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
}

#[test]
fn installation_event_normalizes_uppercase_host() {
    assert_eq!(
        host_from_installation("https://GITHUB.COM/").unwrap(),
        "github.com"
    );
}

#[test]
fn installation_event_accepts_enterprise_host() {
    assert_eq!(
        host_from_installation("https://ghe.example.com/").unwrap(),
        "ghe.example.com"
    );
}

#[test]
fn installation_event_rejects_disallowed_host() {
    let body = installation_payload("https://evil.com/");
    let request = signed_request(SECRET, "installation", &body);
    assert!(matches!(
        converter().event_response(&request),
        Err(BpError::GitHub(GitHubError::HostNotAllowed { host: url })) if url == "https://evil.com/"
    ));
}

#[test]
fn installation_event_rejects_non_https_scheme() {
    assert!(matches!(
        host_from_installation("http://github.com/"),
        Err(BpError::GitHub(GitHubError::HostInvalid { host: url })) if url == "http://github.com/"
    ));
}

#[test]
fn installation_event_accepts_enterprise_host_on_configured_port() {
    let converter = converter_with_hosts(["github.com", "ghe.example.com:8443"]);
    assert_eq!(
        host_from_converter(&converter, "https://ghe.example.com:8443/").unwrap(),
        "ghe.example.com:8443"
    );
}

#[test]
fn installation_event_rejects_enterprise_host_on_unconfigured_port() {
    let converter = converter_with_hosts(["github.com", "ghe.example.com:8443"]);
    for url in [
        "https://ghe.example.com/",
        "https://ghe.example.com:443/",
        "https://ghe.example.com:9999/",
    ] {
        assert!(
            host_from_converter(&converter, url).is_err(),
            "accepted {url}"
        );
    }
}

#[test]
fn installation_event_rejects_github_com_on_custom_port() {
    let converter = converter_with_hosts(["github.com", "ghe.example.com:8443"]);
    assert!(host_from_converter(&converter, "https://github.com:8443/").is_err());
}

#[test]
fn installation_event_rejects_userinfo_trick() {
    assert!(matches!(
        host_from_installation("https://github.com@evil.com/"),
        Err(BpError::GitHub(GitHubError::HostInvalid { host: url })) if url == "https://github.com@evil.com/"
    ));
}
