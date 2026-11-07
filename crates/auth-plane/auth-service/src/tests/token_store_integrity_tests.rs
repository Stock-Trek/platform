use super::support_tests::{OWNER, TestFixture};
use crate::{ApiToken, AuthService, TokenStore};
use async_trait::async_trait;
use platform_errors::{AuthError, BpError, BpResult};
use std::sync::Arc;

struct MismatchedTokenStore {
    token: ApiToken,
}

#[async_trait]
impl TokenStore for MismatchedTokenStore {
    async fn put(&self, _token: &ApiToken) -> BpResult<()> {
        Ok(())
    }

    async fn get(&self, _id: &str) -> BpResult<Option<ApiToken>> {
        Ok(Some(self.token.clone()))
    }

    async fn delete(&self, _id: &str) -> BpResult<()> {
        Ok(())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        Ok(vec!["token-a".to_string()])
    }

    async fn update(&self, _token: &ApiToken) -> BpResult<()> {
        Ok(())
    }
}

struct StoreIntegrityFixture;

impl StoreIntegrityFixture {
    fn service() -> AuthService {
        let store = MismatchedTokenStore {
            token: TestFixture::token("token-b"),
        };
        AuthService::new(Arc::new(store), TestFixture::config()).expect("valid service")
    }

    fn assert_store_error<T>(result: BpResult<T>) {
        assert!(matches!(
            result,
            Err(BpError::Auth(AuthError::Store {
                retryable: false,
                ..
            }))
        ));
    }
}

#[tokio::test]
async fn rejects_a_token_returned_under_a_different_id() {
    let service = StoreIntegrityFixture::service();

    StoreIntegrityFixture::assert_store_error(service.token("token-a").await);
    StoreIntegrityFixture::assert_store_error(service.tokens_for_owner(OWNER).await);
}
