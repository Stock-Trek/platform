use crate::{ApiToken, RevocationReason, RevokedBy, TokenMetadata, TokenScope};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{AuthError, BpError, BpResult};
use secrecy::SecretString;

struct TemporalFixture;

impl TemporalFixture {
    fn metadata(expires_at: DateTime<Utc>) -> TokenMetadata {
        TokenMetadata::new(expires_at, TokenScope::DEFAULT).unwrap()
    }

    fn token(created_at: DateTime<Utc>, metadata: TokenMetadata) -> BpResult<ApiToken> {
        ApiToken::from_parts(
            "token-a",
            "owner-a",
            SecretString::from("secret-a"),
            created_at,
            metadata,
        )
    }

    fn assert_invalid(result: BpResult<ApiToken>, expected: &'static str) {
        let error = result.expect_err("token should be rejected");

        assert!(matches!(
            error,
            BpError::Auth(AuthError::InvalidField { field, .. }) if field == expected
        ));
    }
}

#[test]
fn rejects_expiry_at_or_before_creation() {
    let created_at = Utc::now();

    for expires_at in [
        created_at,
        created_at - Duration::seconds(1),
        created_at - Duration::days(30),
    ] {
        TemporalFixture::assert_invalid(
            TemporalFixture::token(created_at, TemporalFixture::metadata(expires_at)),
            "expires_at",
        );
    }
}

#[test]
fn rejects_a_revocation_before_creation() {
    let created_at = Utc::now();
    let mut metadata = TemporalFixture::metadata(created_at + Duration::hours(1));
    metadata.revoked_at = Some(created_at - Duration::seconds(1));
    metadata.revocation_reason = Some(RevocationReason::Compromised);
    metadata.revoked_by = Some(RevokedBy::SecurityTeam);

    TemporalFixture::assert_invalid(TemporalFixture::token(created_at, metadata), "revoked_at");
}

#[test]
fn rejects_rotation_overlap_before_creation() {
    let created_at = Utc::now();
    let mut metadata = TemporalFixture::metadata(created_at + Duration::hours(1));
    metadata.replaced_by = Some("token-b".to_string());
    metadata.rotation_overlap_until = Some(created_at - Duration::seconds(1));

    TemporalFixture::assert_invalid(
        TemporalFixture::token(created_at, metadata),
        "rotation_overlap_until",
    );
}

#[test]
fn accepts_revocation_at_the_creation_boundary() {
    let created_at = Utc::now();
    let mut metadata = TemporalFixture::metadata(created_at + Duration::hours(1));
    metadata.revoked_at = Some(created_at);
    metadata.revocation_reason = Some(RevocationReason::Compromised);
    metadata.revoked_by = Some(RevokedBy::SecurityTeam);

    let token = TemporalFixture::token(created_at, metadata).expect("temporal state is valid");

    assert_eq!(token.metadata().revoked_at(), Some(created_at));
}
