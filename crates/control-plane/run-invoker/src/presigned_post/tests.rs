use super::PresignedPostSigner;
use aws_credential_types::Credentials;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};

fn credentials(session_token: Option<String>) -> Credentials {
    Credentials::new(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        session_token,
        None,
        "test",
    )
}

fn now() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_704_164_645)
}

#[test]
fn signs_post_policy_for_the_log_key() {
    let signer = PresignedPostSigner::new("run-logs".to_string(), "us-east-1".to_string(), None);
    let credentials = credentials(Some("token/with+chars=".to_string()));

    let post = signer
        .sign(
            &credentials,
            "logs/demo-algorithm/20260203T040506.789Z.log",
            now(),
            Duration::from_secs(900),
        )
        .unwrap();

    assert_eq!(post.url, "https://run-logs.s3.us-east-1.amazonaws.com/");
    assert_eq!(
        post.fields["key"],
        "logs/demo-algorithm/20260203T040506.789Z.log"
    );
    assert_eq!(post.fields["x-amz-date"], "20240102T030405Z");
    assert_eq!(
        post.fields["x-amz-credential"],
        "AKIAIOSFODNN7EXAMPLE/20240102/us-east-1/s3/aws4_request"
    );
    assert_eq!(post.fields["x-amz-security-token"], "token/with+chars=");
    assert_eq!(post.fields["x-amz-signature"].len(), 64);

    let policy: Value =
        serde_json::from_slice(&BASE64.decode(&post.fields["policy"]).unwrap()).unwrap();
    assert_eq!(policy["expiration"], "2024-01-02T03:19:05.000Z");
    assert_eq!(
        policy["conditions"][1],
        json!({ "key": "logs/demo-algorithm/20260203T040506.789Z.log" })
    );
    assert_eq!(
        policy["conditions"][5],
        json!({ "x-amz-security-token": "token/with+chars=" })
    );
}

#[test]
fn signs_without_a_session_token_against_a_custom_endpoint() {
    let signer = PresignedPostSigner::new(
        "run-logs".to_string(),
        "eu-west-1".to_string(),
        Some("http://localhost:4566".to_string()),
    );
    let credentials = credentials(None);

    let post = signer
        .sign(
            &credentials,
            "logs/demo-algorithm/20260203T040506.789Z.log",
            now(),
            Duration::from_secs(60),
        )
        .unwrap();

    assert_eq!(post.url, "http://localhost:4566/run-logs/");
    assert!(!post.fields.contains_key("x-amz-security-token"));
    let policy: Value =
        serde_json::from_slice(&BASE64.decode(&post.fields["policy"]).unwrap()).unwrap();
    assert!(
        !policy["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|condition| condition.get("x-amz-security-token").is_some())
    );
}

#[test]
fn addresses_chinese_regions_with_the_chinese_dns_suffix() {
    let signer = PresignedPostSigner::new("run-logs".to_string(), "cn-north-1".to_string(), None);
    let credentials = credentials(None);

    let post = signer
        .sign(
            &credentials,
            "logs/a/20260203T040506.789Z.log",
            now(),
            Duration::from_secs(60),
        )
        .unwrap();

    assert_eq!(post.url, "https://run-logs.s3.cn-north-1.amazonaws.com.cn/");
}
