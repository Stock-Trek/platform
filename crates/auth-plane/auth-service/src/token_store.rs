use crate::{
    ApiToken, TokenAuditEvent, auth_service::MAX_UPDATE_ATTEMPTS, owner_index::OwnerIndex,
    owner_index::OwnerIndexEntry, token_audit_outbox::TokenAuditOutbox,
    token_rotation_transaction::TokenRotationTransaction,
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};

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
    async fn put(&self, token: &ApiToken) -> PlatformResult<()>;

    async fn get(&self, id: &str) -> PlatformResult<Option<ApiToken>>;

    async fn delete(&self, id: &str) -> PlatformResult<()>;

    async fn delete_if(&self, expected: &ApiToken) -> PlatformResult<bool> {
        let Some(current) = self.get(expected.id()).await? else {
            return Ok(false);
        };
        if current.to_value() != expected.to_value() {
            return Ok(false);
        }
        self.delete(expected.id()).await?;
        Ok(true)
    }

    async fn ids(&self) -> PlatformResult<Vec<String>>;

    async fn owner_index(&self, _owner: &str) -> PlatformResult<Option<OwnerIndex>> {
        Ok(None)
    }

    async fn put_owner_index(&self, _index: &OwnerIndex) -> PlatformResult<()> {
        Ok(())
    }

    async fn owner_indexes(&self) -> PlatformResult<Vec<OwnerIndex>> {
        Ok(Vec::new())
    }

    async fn index_owner_token(&self, token: &ApiToken) -> PlatformResult<()> {
        let mut index = self
            .owner_index(token.owner())
            .await?
            .unwrap_or_else(|| OwnerIndex::new(token.owner()));
        if index.owner() != token.owner() {
            return Err(PlatformError::auth_store(
                "write",
                OwnerIndex::storage_key(token.owner()),
                false,
                "owner index belongs to a different owner".to_string(),
            ));
        }
        index.upsert(OwnerIndexEntry::from_token(token));
        self.put_owner_index(&index).await
    }

    async fn remove_owner_index_token(&self, owner: &str, id: &str) -> PlatformResult<()> {
        let Some(mut index) = self.owner_index(owner).await? else {
            return Ok(());
        };
        index.remove(id);
        self.put_owner_index(&index).await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> PlatformResult<()>;

    /// Applies `mutation` and records `event` in the same token document.
    ///
    /// Implementations must make the mutation and the outbox append a single
    /// compare-and-swap write. Stores must override this method; there is no
    /// safe default because silently discarding the audit event would lose
    /// audit data.
    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent)
    -> PlatformResult<bool>;

    /// Applies `mutation` and records all `events` as one durable operation
    /// where the store supports it.
    ///
    /// The default preserves at-least-once audit delivery by committing the
    /// mutation with the first event and appending any remaining events.
    async fn commit_events(
        &self,
        mutation: TokenMutation,
        events: Vec<TokenAuditEvent>,
    ) -> PlatformResult<bool> {
        let mut events = events.into_iter();
        let Some(first) = events.next() else {
            return Ok(false);
        };
        if !self.commit(mutation, first).await? {
            return Ok(false);
        }
        for event in events {
            self.append_audit(event).await?;
        }
        Ok(true)
    }

    /// Appends an audit event to the outbox without changing token state.
    ///
    /// Stores that can keep events independently of a token should override
    /// this. The default attaches the event to an existing token so stores that
    /// only implement transactional commits can still preserve events for known
    /// tokens.
    async fn append_audit(&self, mut event: TokenAuditEvent) -> PlatformResult<()> {
        TokenAuditOutbox::ensure_event_id(&mut event);
        let Some(token_id) = event.token_id.clone() else {
            return Err(PlatformError::auth_store(
                "audit",
                "outbox",
                false,
                "token store does not support standalone audit events".to_string(),
            ));
        };
        let Some(token) = self.get(&token_id).await? else {
            return Err(PlatformError::auth_store(
                "audit",
                "outbox",
                false,
                "token store does not support standalone audit events".to_string(),
            ));
        };
        if !self
            .commit(
                TokenMutation::Update {
                    expected: token.clone(),
                    token,
                },
                event,
            )
            .await?
        {
            return Err(PlatformError::token_conflict(token_id));
        }
        Ok(())
    }

    /// Atomically writes a rotation transaction record, including its events.
    ///
    /// This must be a single durable write so a rotation can never be
    /// materialized without its audit events. Stores that cannot keep a
    /// separate transaction record must still persist the events before they
    /// materialize the rotation; they must not silently drop
    /// [`TokenRotationTransaction::events`].
    async fn commit_rotation(&self, transaction: &TokenRotationTransaction) -> PlatformResult<()>;

    /// Applies a committed rotation transaction idempotently.
    ///
    /// This only changes token state. Audit events are owned by
    /// [`TokenStore::commit_rotation`] and must already be persisted by the
    /// time this method is called.
    async fn materialize_rotation(
        &self,
        transaction: &TokenRotationTransaction,
    ) -> PlatformResult<()> {
        let replacement = transaction.replacement();
        let existing = match self.get(replacement.id()).await? {
            Some(existing) => Some(existing),
            None => match self.put(replacement).await {
                Ok(()) => None,
                Err(PlatformError::Auth(platform_errors::AuthError::TokenExists { .. })) => Some(
                    self.get(replacement.id())
                        .await?
                        .ok_or_else(|| PlatformError::token_conflict(replacement.id()))?,
                ),
                Err(error) => return Err(error),
            },
        };
        if let Some(existing) = existing {
            if existing.owner() != replacement.owner()
                || existing.metadata().rotated_from() != Some(transaction.original().id())
            {
                return Err(PlatformError::token_conflict(replacement.id()));
            }
            if let Some(next) = existing.metadata().replaced_by() {
                return Err(PlatformError::auth_invalid_field(
                    "replacement_id",
                    format!("has already been rotated to {next:?}"),
                ));
            }
            if existing.metadata().is_revoked() {
                return Err(PlatformError::auth_revoked(replacement.id()));
            }
            let now = transaction.timestamp();
            if existing.created_at() > now {
                return Err(PlatformError::auth_invalid_field(
                    "created_at",
                    "must not be in the future",
                ));
            }
            if existing.metadata().is_expired(now) {
                return Err(PlatformError::auth_expired(replacement.id()));
            }
            if existing.metadata().is_rotation_overlap_ended(now) {
                return Err(PlatformError::auth_rotation_overlap_ended(replacement.id()));
            }
            if !existing.same_rotation_replacement(replacement) {
                return Err(PlatformError::token_conflict(replacement.id()));
            }
        }

        let mut current = self
            .get(transaction.original().id())
            .await?
            .ok_or_else(|| PlatformError::unknown_token(transaction.original().id()))?;
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            if current.metadata().replaced_by() == Some(replacement.id()) {
                return Ok(());
            }
            if let Some(next) = current.metadata().replaced_by() {
                return Err(PlatformError::auth_invalid_field(
                    "token_id",
                    format!("has already been rotated to {next:?}"),
                ));
            }
            let overlap_until = transaction
                .retired()
                .metadata()
                .rotation_overlap_until()
                .ok_or_else(|| {
                    PlatformError::auth_invalid_field(
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
                Err(PlatformError::Auth(platform_errors::AuthError::Conflict { .. })) => {
                    current = self
                        .get(transaction.original().id())
                        .await?
                        .ok_or_else(|| PlatformError::unknown_token(transaction.original().id()))?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(PlatformError::token_conflict(transaction.original().id()))
    }

    async fn pending_audits(&self) -> PlatformResult<Vec<TokenAuditEvent>> {
        Ok(Vec::new())
    }

    async fn acknowledge_audit(&self, _event_id: &str) -> PlatformResult<()> {
        Ok(())
    }

    async fn pending_rotations(&self) -> PlatformResult<Vec<TokenRotationTransaction>> {
        Ok(Vec::new())
    }

    async fn acknowledge_rotation(&self, _transaction_id: &str) -> PlatformResult<()> {
        Ok(())
    }
}
