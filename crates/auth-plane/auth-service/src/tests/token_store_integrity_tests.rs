use super::support_tests::{AuthServiceBuilder, OWNER, TestFixture};
use crate::{
    ApiToken, AuthService, OwnerIndex, TokenAuditEvent, TokenMetadata, TokenMutation,
    TokenRotationTransaction, TokenScope, TokenStore,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
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

    async fn owner_index(&self, owner: &str) -> BpResult<Option<OwnerIndex>> {
        let token = ApiToken::generate(
            "token-a",
            owner,
            self.token.created_at(),
            self.token.metadata().clone(),
        )?;
        Ok(Some(OwnerIndex::from_tokens(owner, &[token])))
    }

    async fn commit_rotation(&self, _transaction: &TokenRotationTransaction) -> BpResult<()> {
        Err(BpError::auth_store(
            "write",
            "rotation-transaction",
            false,
            "store does not support rotation transactions".to_string(),
        ))
    }

    async fn update(&self, _expected: &ApiToken, _token: &ApiToken) -> BpResult<()> {
        Ok(())
    }

    async fn commit(&self, _mutation: TokenMutation, _event: TokenAuditEvent) -> BpResult<bool> {
        Ok(true)
    }
}

struct StoreIntegrityFixture;

impl StoreIntegrityFixture {
    fn service() -> AuthService {
        let store = MismatchedTokenStore {
            token: TestFixture::token("token-b"),
        };
        AuthServiceBuilder::new().store(Arc::new(store)).build()
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

#[tokio::test]
async fn rejects_corrupt_metadata_returned_by_a_store() {
    let mut token = TestFixture::token("token-a");
    token.metadata.revoked_at = Some(Utc::now());
    let service = AuthServiceBuilder::new()
        .store(Arc::new(MismatchedTokenStore { token }))
        .build();

    StoreIntegrityFixture::assert_store_error(service.token("token-a").await);
}

struct DuplicateIdsStore {
    token: ApiToken,
}

#[async_trait]
impl TokenStore for DuplicateIdsStore {
    async fn put(&self, _token: &ApiToken) -> BpResult<()> {
        Ok(())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        Ok((id == self.token.id()).then(|| self.token.clone()))
    }

    async fn delete(&self, _id: &str) -> BpResult<()> {
        Ok(())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        Ok(vec![
            self.token.id().to_string(),
            self.token.id().to_string(),
        ])
    }

    async fn commit_rotation(&self, _transaction: &TokenRotationTransaction) -> BpResult<()> {
        Err(BpError::auth_store(
            "write",
            "rotation-transaction",
            false,
            "store does not support rotation transactions".to_string(),
        ))
    }

    async fn update(&self, _expected: &ApiToken, _token: &ApiToken) -> BpResult<()> {
        Ok(())
    }

    async fn commit(&self, _mutation: TokenMutation, _event: TokenAuditEvent) -> BpResult<bool> {
        Ok(true)
    }
}

struct DuplicateIdsFixture;

impl DuplicateIdsFixture {
    fn service(token: ApiToken) -> AuthService {
        AuthServiceBuilder::new()
            .store(Arc::new(DuplicateIdsStore { token }))
            .build()
    }

    fn expired_token() -> ApiToken {
        let created_at = Utc::now() - Duration::seconds(2);
        let metadata = TokenMetadata::new(Utc::now() - Duration::seconds(1), TokenScope::DEFAULT)
            .expect("valid metadata");
        ApiToken::generate("token-a", OWNER, created_at, metadata).expect("valid token")
    }
}

#[tokio::test]
async fn purging_deduplicates_store_ids() {
    let service = DuplicateIdsFixture::service(DuplicateIdsFixture::expired_token());

    let purged = service.purge_expired_tokens(Utc::now()).await.unwrap();

    assert_eq!(purged, 1);
}
