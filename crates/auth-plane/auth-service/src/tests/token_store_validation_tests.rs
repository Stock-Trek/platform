use super::support_tests::TestFixture;
use crate::{ApiToken, MemoryTokenStore, TokenStore};
use chrono::Utc;
use platform_errors::{AuthError, BpError};

struct StoreValidationFixture;

impl StoreValidationFixture {
    fn invalid_token() -> ApiToken {
        let mut token = TestFixture::token("token-a");
        token.metadata.revoked_at = Some(Utc::now());
        token
    }

    fn assert_invalid_metadata<T>(result: Result<T, BpError>) {
        assert!(matches!(
            result,
            Err(BpError::Auth(AuthError::InvalidField {
                field: "revocation_reason",
                ..
            }))
        ));
    }
}

#[tokio::test]
async fn memory_store_rejects_invalid_metadata_on_insert() {
    let store = MemoryTokenStore::default();

    StoreValidationFixture::assert_invalid_metadata(
        store.put(&StoreValidationFixture::invalid_token()).await,
    );
}

#[tokio::test]
async fn memory_store_rejects_invalid_metadata_on_update() {
    let store = MemoryTokenStore::default();
    let token = TestFixture::token("token-a");
    store.put(&token).await.unwrap();
    let mut invalid = token.clone();
    invalid.metadata.revoked_at = Some(Utc::now());

    StoreValidationFixture::assert_invalid_metadata(store.update(&invalid).await);
}
