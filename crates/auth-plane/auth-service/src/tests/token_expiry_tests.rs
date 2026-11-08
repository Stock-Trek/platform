use crate::{TokenMetadata, TokenScope};
use chrono::{Duration, Utc};

struct TokenExpiryFixture;

impl TokenExpiryFixture {
    fn rotated_metadata(now: chrono::DateTime<Utc>) -> TokenMetadata {
        let mut metadata =
            TokenMetadata::new(now + Duration::hours(1), TokenScope::DEFAULT).unwrap();
        metadata.begin_rotation("token-b", now + Duration::seconds(30));
        metadata
    }
}

#[test]
fn expiry_uses_the_token_expiry_not_the_rotation_overlap() {
    let now = Utc::now();
    let metadata = TokenExpiryFixture::rotated_metadata(now);
    let after_overlap = now + Duration::seconds(31);

    assert!(!metadata.is_expired(after_overlap));
    assert!(!metadata.is_active(after_overlap));
}

#[test]
fn expired_tokens_are_expired_after_the_rotation_overlap() {
    let now = Utc::now();
    let metadata = TokenExpiryFixture::rotated_metadata(now);

    assert!(metadata.is_expired(now + Duration::hours(1)));
    assert!(!metadata.is_active(now + Duration::hours(1)));
}
