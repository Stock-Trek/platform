use crate::{SigV4Config, SigV4Registry};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

struct Fixture;

impl Fixture {
    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }
}

#[test]
fn parses_registry_entries_with_session_tokens() {
    let registry = SigV4Registry::from_json(
        r#"[{"accessKeyId":"AKID","secretAccessKey":"secret","owner":"owner","sessionToken":"token"}]"#,
    )
    .unwrap();
    let signer = SigV4Config::new(registry, "us-east-1", "service")
        .unwrap()
        .signer_for_owner("owner")
        .unwrap();
    let mut headers = BTreeMap::new();
    signer
        .sign(
            "GET",
            "https://example.com/path",
            &mut headers,
            b"",
            Fixture::now(),
        )
        .unwrap();

    assert_eq!(headers.get("x-amz-security-token").unwrap(), "token");
}

#[test]
fn rejects_duplicate_owners() {
    let result = SigV4Registry::from_json(
        r#"[{"accessKeyId":"A","secretAccessKey":"a","owner":"owner"},{"accessKeyId":"B","secretAccessKey":"b","owner":"owner"}]"#,
    );

    assert!(result.is_err());
}

#[test]
fn rejects_an_owner_without_credentials() {
    let registry = SigV4Registry::from_parts("AKIDEXAMPLE", "secret-key", "owner-a", None).unwrap();
    assert!(
        SigV4Config::new(registry, "eu-west-1", "execute-api")
            .unwrap()
            .signer_for_owner("missing-owner")
            .is_err()
    );
}
