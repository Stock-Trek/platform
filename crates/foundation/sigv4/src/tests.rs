use crate::{SigV4Config, SigV4Registry, SigV4Request};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

struct Fixture;

impl Fixture {
    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET_KEY: &str = "secret-key";
    const OWNER: &str = "owner-a";
    const NOW_SECS: u64 = 1_700_000_000;

    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(Self::NOW_SECS)
    }

    fn registry() -> SigV4Registry {
        SigV4Registry::from_parts(Self::ACCESS_KEY, Self::SECRET_KEY, Self::OWNER, None).unwrap()
    }

    fn config() -> SigV4Config {
        SigV4Config::new(Self::registry(), "eu-west-1", "execute-api").unwrap()
    }

    fn headers() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("x-owner".to_string(), Self::OWNER.to_string()),
        ])
    }
}

#[test]
fn signs_and_verifies_a_request() {
    let config = Fixture::config();
    let mut headers = Fixture::headers();
    let body = br#"{"owner":"owner-a"}"#;
    config
        .signer_for_owner(Fixture::OWNER)
        .unwrap()
        .sign(
            "POST",
            "http://127.0.0.1:8080/jwt",
            &mut headers,
            body,
            Fixture::now(),
        )
        .unwrap();

    let caller = config
        .verify(
            &SigV4Request::new("POST", "http://127.0.0.1:8080/jwt", &headers, body),
            Fixture::now(),
        )
        .unwrap();

    assert_eq!(caller.owner(), Fixture::OWNER);
    assert_eq!(caller.access_key_id(), Fixture::ACCESS_KEY);
    assert!(headers.contains_key("authorization"));
    assert!(headers.contains_key("host"));
}

#[test]
fn rejects_a_tampered_body() {
    let config = Fixture::config();
    let mut headers = Fixture::headers();
    config
        .signer_for_owner(Fixture::OWNER)
        .unwrap()
        .sign(
            "POST",
            "http://127.0.0.1:8080/jwt",
            &mut headers,
            br#"{"owner":"owner-a"}"#,
            Fixture::now(),
        )
        .unwrap();

    assert!(
        config
            .verify(
                &SigV4Request::new(
                    "POST",
                    "http://127.0.0.1:8080/jwt",
                    &headers,
                    br#"{"owner":"owner-b"}"#,
                ),
                Fixture::now(),
            )
            .is_err()
    );
}

#[test]
fn rejects_a_tampered_signed_header() {
    let config = Fixture::config();
    let mut headers = Fixture::headers();
    config
        .signer_for_owner(Fixture::OWNER)
        .unwrap()
        .sign(
            "POST",
            "http://127.0.0.1:8080/jwt",
            &mut headers,
            b"{}",
            Fixture::now(),
        )
        .unwrap();
    headers.insert("x-owner".to_string(), "owner-b".to_string());

    assert!(
        config
            .verify(
                &SigV4Request::new("POST", "http://127.0.0.1:8080/jwt", &headers, b"{}"),
                Fixture::now(),
            )
            .is_err()
    );
}

#[test]
fn rejects_a_stale_request() {
    let config = Fixture::config();
    let mut headers = Fixture::headers();
    config
        .signer_for_owner(Fixture::OWNER)
        .unwrap()
        .sign(
            "POST",
            "http://127.0.0.1:8080/jwt",
            &mut headers,
            b"{}",
            Fixture::now(),
        )
        .unwrap();

    let stale = Fixture::now() + Duration::from_secs(301);
    assert!(
        config
            .verify(
                &SigV4Request::new("POST", "http://127.0.0.1:8080/jwt", &headers, b"{}"),
                stale,
            )
            .is_err()
    );
}

#[test]
fn rejects_an_unknown_access_key() {
    let mut headers = Fixture::headers();
    Fixture::config()
        .signer_for_owner(Fixture::OWNER)
        .unwrap()
        .sign(
            "POST",
            "http://127.0.0.1:8080/jwt",
            &mut headers,
            b"{}",
            Fixture::now(),
        )
        .unwrap();
    let registry = SigV4Registry::from_parts("OTHER", "other-secret", "owner-b", None).unwrap();
    let config = SigV4Config::new(registry, "eu-west-1", "execute-api").unwrap();

    assert!(
        config
            .verify(
                &SigV4Request::new("POST", "http://127.0.0.1:8080/jwt", &headers, b"{}"),
                Fixture::now(),
            )
            .is_err()
    );
}

#[test]
fn verifies_a_request_with_a_session_token() {
    let registry = SigV4Registry::from_parts(
        Fixture::ACCESS_KEY,
        Fixture::SECRET_KEY,
        Fixture::OWNER,
        Some("session-token".to_string()),
    )
    .unwrap();
    let config = SigV4Config::new(registry, "eu-west-1", "execute-api").unwrap();
    let mut headers = Fixture::headers();
    config
        .signer_for_owner(Fixture::OWNER)
        .unwrap()
        .sign(
            "POST",
            "http://127.0.0.1:8080/jwt",
            &mut headers,
            b"{}",
            Fixture::now(),
        )
        .unwrap();

    let caller = config
        .verify(
            &SigV4Request::new("POST", "http://127.0.0.1:8080/jwt", &headers, b"{}"),
            Fixture::now(),
        )
        .unwrap();

    assert_eq!(caller.owner(), Fixture::OWNER);
}
