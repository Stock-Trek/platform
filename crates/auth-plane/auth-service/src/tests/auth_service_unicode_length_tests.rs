use super::support_tests::TestFixture;
use crate::{
    ApiToken, OrderRequestContext, SignedOrderRequest,
    api_token::{MAX_OWNER_LENGTH, MAX_SECRET_LENGTH},
    signed_order_request::MAX_ENVELOPE_LENGTH,
};
use chrono::{Duration, Utc};
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

    fn signed_envelope_with_length(length: usize) -> BpResult<SignedOrderRequest> {
        let issued_at = chrono::DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap();
        let context = OrderRequestContext::new(
            "token-a",
            issued_at,
            issued_at + Duration::minutes(5),
            "nonce-a",
        )?;
        let prefix_length = SignedOrderRequest::from_context(&context, "signature")?
            .envelope()
            .rsplit_once('.')
            .unwrap()
            .0
            .chars()
            .count();
        SignedOrderRequest::from_context(
            &context,
            UNICODE_CHARACTER.repeat(length.saturating_sub(prefix_length + 1)),
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

#[test]
fn counts_envelope_length_in_characters() {
    let at_limit = UnicodeLengthFixture::signed_envelope_with_length(MAX_ENVELOPE_LENGTH)
        .expect("an envelope at the character limit is valid");
    assert_eq!(at_limit.envelope().chars().count(), MAX_ENVELOPE_LENGTH);
    assert!(at_limit.envelope().len() > MAX_ENVELOPE_LENGTH);

    let over_limit = UnicodeLengthFixture::signed_envelope_with_length(MAX_ENVELOPE_LENGTH + 1);
    assert!(over_limit.is_err());
}
