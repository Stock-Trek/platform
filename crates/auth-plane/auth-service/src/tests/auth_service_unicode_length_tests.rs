use super::support_tests::TestFixture;
use crate::{
    ApiToken,
    api_token::{MAX_OWNER_LENGTH, MAX_SECRET_LENGTH},
};
use chrono::Utc;
use platform_errors::BpResult;
use secrecy::SecretString;

const UNICODE_CHARACTER: &str = "é";

struct UnicodeLengthFixture;

impl UnicodeLengthFixture {
    fn token_with_owner(owner: &str) -> BpResult<ApiToken> {
        ApiToken::generate("token-a", owner, Utc::now(), TestFixture::metadata())
    }

    fn token_with_secret(secret: &str) -> BpResult<ApiToken> {
        ApiToken::from_parts(
            "token-a",
            "demo-owner",
            SecretString::from(secret),
            Utc::now(),
            TestFixture::metadata(),
        )
    }
}

#[test]
fn counts_owner_length_in_characters() {
    let at_limit = UNICODE_CHARACTER.repeat(MAX_OWNER_LENGTH);
    assert!(at_limit.len() > MAX_OWNER_LENGTH);
    assert!(UnicodeLengthFixture::token_with_owner(&at_limit).is_ok());

    let over_limit = UNICODE_CHARACTER.repeat(MAX_OWNER_LENGTH + 1);
    assert!(UnicodeLengthFixture::token_with_owner(&over_limit).is_err());
}

#[test]
fn counts_secret_length_in_characters() {
    let at_limit = UNICODE_CHARACTER.repeat(MAX_SECRET_LENGTH);
    assert!(at_limit.len() > MAX_SECRET_LENGTH);
    assert!(UnicodeLengthFixture::token_with_secret(&at_limit).is_ok());

    let over_limit = UNICODE_CHARACTER.repeat(MAX_SECRET_LENGTH + 1);
    assert!(UnicodeLengthFixture::token_with_secret(&over_limit).is_err());
}
