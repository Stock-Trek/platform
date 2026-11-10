use crate::{
    ApiToken, TokenAuditEvent, TokenRotationTransaction, auth_service::MAX_UPDATE_ATTEMPTS,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};

/// A single token mutation that can be committed together with an audit event.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum TokenMutation {
    Create(ApiToken),
    Update { expected: ApiToken, token: ApiToken },
    Delete { expected: ApiToken },
}

#[async_trait]
pub trait TokenStore: Send + Sync {
    async fn put(&self, token: &ApiToken) -> BpResult<()>;

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>>;

    async fn delete(&self, id: &str) -> BpResult<()>;

    async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        let Some(current) = self.get(expected.id()).await? else {
            return Ok(false);
        };
        if current.to_value() != expected.to_value() {
            return Ok(false);
        }
        self.delete(expected.id()).await?;
        Ok(true)
    }

    async fn ids(&self) -> BpResult<Vec<String>>;

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()>;

    /// Applies `mutation` and records `event` in the same token document.
    ///
    /// Implementations must make the mutation and the outbox append a single
    /// compare-and-swap write. The default keeps third-party stores working,
    /// but cannot provide the outbox guarantee.
    async fn commit(&self, mutation: TokenMutation, _event: TokenAuditEvent) -> BpResult<bool> {
        match mutation {
            TokenMutation::Create(token) => {
                self.put(&token).await?;
                Ok(true)
            }
            TokenMutation::Update { expected, token } => {
                self.update(&expected, &token).await?;
                Ok(true)
            }
            TokenMutation::Delete { expected } => self.delete_if(&expected).await,
        }
    }

    /// Atomically writes a rotation transaction record.
    ///
    /// Stores that cannot keep a separate transaction record materialize it
    /// immediately instead.
    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        self.materialize_rotation(transaction).await
    }

    /// Applies a committed rotation transaction idempotently.
    async fn materialize_rotation(&self, transaction: &TokenRotationTransaction) -> BpResult<()> {
        let replacement = transaction.replacement();
        if let Some(existing) = self.get(replacement.id()).await? {
            if existing.owner() != replacement.owner()
                || existing.metadata().rotated_from() != Some(transaction.original().id())
            {
                return Err(BpError::token_conflict(replacement.id()));
            }
            if let Some(next) = existing.metadata().replaced_by() {
                return Err(BpError::auth_invalid_field(
                    "replacement_id",
                    format!("has already been rotated to {next:?}"),
                ));
            }
            if existing.metadata().is_revoked() {
                return Err(BpError::auth_revoked(replacement.id()));
            }
            let now = transaction.timestamp();
            if existing.created_at() > now {
                return Err(BpError::auth_invalid_field(
                    "created_at",
                    "must not be in the future",
                ));
            }
            if existing.metadata().is_expired(now) {
                return Err(BpError::auth_expired(replacement.id()));
            }
            if existing.metadata().is_rotation_overlap_ended(now) {
                return Err(BpError::auth_rotation_overlap_ended(replacement.id()));
            }
        } else {
            match self.put(replacement).await {
                Ok(()) => {}
                Err(BpError::Auth(platform_errors::AuthError::TokenExists { .. })) => {}
                Err(error) => return Err(error),
            }
        }

        let mut current = self
            .get(transaction.original().id())
            .await?
            .ok_or_else(|| BpError::unknown_token(transaction.original().id()))?;
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            if current.metadata().replaced_by() == Some(replacement.id()) {
                return Ok(());
            }
            if let Some(next) = current.metadata().replaced_by() {
                return Err(BpError::auth_invalid_field(
                    "token_id",
                    format!("has already been rotated to {next:?}"),
                ));
            }
            let overlap_until = transaction
                .retired()
                .metadata()
                .rotation_overlap_until()
                .ok_or_else(|| {
                    BpError::auth_invalid_field(
                        "rotation_overlap_until",
                        "is missing from the rotation transaction",
                    )
                })?;
            let mut retired = current.clone();
            retired
                .metadata
                .begin_rotation(replacement.id(), overlap_until);
            retired.validate()?;
            match self.update(&current, &retired).await {
                Ok(()) => return Ok(()),
                Err(BpError::Auth(platform_errors::AuthError::Conflict { .. })) => {
                    current = self
                        .get(transaction.original().id())
                        .await?
                        .ok_or_else(|| BpError::unknown_token(transaction.original().id()))?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(BpError::token_conflict(transaction.original().id()))
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        Ok(Vec::new())
    }

    async fn acknowledge_audit(&self, _event_id: &str) -> BpResult<()> {
        Ok(())
    }

    async fn pending_rotations(&self) -> BpResult<Vec<TokenRotationTransaction>> {
        Ok(Vec::new())
    }

    async fn acknowledge_rotation(&self, _transaction_id: &str) -> BpResult<()> {
        Ok(())
    }
}
