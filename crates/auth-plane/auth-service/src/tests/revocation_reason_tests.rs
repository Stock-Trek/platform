use super::support_tests::TestFixture;
use crate::{ApiToken, RevocationReason};
use chrono::Utc;
use platform_errors::{AuthError, BpError};
use serde_json::json;

struct RevocationReasonFixture;

impl RevocationReasonFixture {
    fn reasons() -> [RevocationReason; 6] {
        [
            RevocationReason::Compromised,
            RevocationReason::Expired,
            RevocationReason::NoLongerNeeded,
            RevocationReason::OwnerRequested,
            RevocationReason::Rotated,
            RevocationReason::Administrative,
        ]
    }
}

#[test]
fn reasons_have_stable_string_representations() {
    let expected = [
        (RevocationReason::Compromised, "compromised"),
        (RevocationReason::Expired, "expired"),
        (RevocationReason::NoLongerNeeded, "no_longer_needed"),
        (RevocationReason::OwnerRequested, "owner_requested"),
        (RevocationReason::Rotated, "rotated"),
        (RevocationReason::Administrative, "administrative"),
    ];

    for (reason, value) in expected {
        assert_eq!(reason.as_str(), value);
    }
}

#[test]
fn reasons_round_trip_through_their_string_representation() {
    for reason in RevocationReasonFixture::reasons() {
        assert_eq!(reason.as_str().parse::<RevocationReason>().unwrap(), reason);
    }
}

#[test]
fn rejects_unknown_reason_strings() {
    let error = "other".parse::<RevocationReason>().unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField {
            field: "revocation_reason",
            ..
        })
    ));
}

#[test]
fn persists_the_reason_through_a_stored_token_value() {
    let mut token = TestFixture::token("token-a");
    let now = Utc::now();
    token.metadata.revoke(
        now,
        RevocationReason::Compromised,
        "security-team".to_string(),
    );

    let value = token.to_value();
    let restored = ApiToken::from_value(token.id(), &value).unwrap();

    assert_eq!(value.get("revocation_reason"), Some(&json!("compromised")));
    assert_eq!(
        restored.metadata().revocation_reason(),
        Some(RevocationReason::Compromised)
    );
}
