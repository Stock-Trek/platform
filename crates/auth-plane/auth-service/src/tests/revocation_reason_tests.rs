use super::support_tests::TestFixture;
use crate::{ApiToken, RevocationReason, RevokedBy};
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
        assert_eq!(reason.to_string(), value);
    }
}

#[test]
fn reasons_round_trip_through_their_string_representation() {
    for reason in RevocationReasonFixture::reasons() {
        assert_eq!(
            reason.to_string().parse::<RevocationReason>().unwrap(),
            reason
        );
    }
}

#[test]
fn rejects_unknown_reason_strings() {
    assert!("other".parse::<RevocationReason>().is_err());
}

#[test]
fn persists_the_reason_and_actor_through_a_stored_token_value() {
    let mut token = TestFixture::token("token-a");
    let now = Utc::now();
    token
        .metadata
        .revoke(now, RevocationReason::Compromised, RevokedBy::SecurityTeam);

    let value = token.to_value();
    let restored = ApiToken::from_value(token.id(), &value).unwrap();

    assert_eq!(value.get("revocation_reason"), Some(&json!("compromised")));
    assert_eq!(value.get("revoked_by"), Some(&json!("security_team")));
    assert_eq!(
        restored.metadata().revocation_reason(),
        Some(RevocationReason::Compromised)
    );
    assert_eq!(
        restored.metadata().revoked_by(),
        Some(RevokedBy::SecurityTeam)
    );
}

#[test]
fn rejects_unknown_actor_strings() {
    let mut value = TestFixture::token("token-a").to_value();
    value["revoked_by"] = json!("unknown");

    let error = ApiToken::from_value("token-a", &value).unwrap_err();

    assert!(matches!(
        error,
        BpError::Auth(AuthError::InvalidField {
            field: "revoked_by",
            ..
        })
    ));
}
