use crate::{ApiToken, AuthService, OwnerIndex, OwnerIndexEntry, TokenMutation, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use std::collections::BTreeMap;

impl AuthService {
    pub async fn reconcile_owner_indexes(&self) -> PlatformResult<usize> {
        let mut ids = self.store.ids().await?;
        ids.sort();
        ids.dedup();
        let mut tokens_by_owner: BTreeMap<String, Vec<ApiToken>> = BTreeMap::new();
        for id in ids {
            let Some(token) = self.stored_token(&id).await? else {
                continue;
            };
            tokens_by_owner
                .entry(token.owner().to_string())
                .or_default()
                .push(token);
        }
        let existing = self.store.owner_indexes().await?;
        let mut written = 0;
        for (owner, tokens) in &tokens_by_owner {
            self.store
                .put_owner_index(&OwnerIndex::from_tokens(owner, tokens))
                .await?;
            written += 1;
        }
        for index in existing {
            if tokens_by_owner.contains_key(index.owner()) {
                continue;
            }
            self.store
                .put_owner_index(&OwnerIndex::new(index.owner()))
                .await?;
            written += 1;
        }
        Ok(written)
    }

    pub async fn sync_owner_index_token(&self, token: &ApiToken) {
        if let Err(error) = self.store.index_owner_token(token).await {
            tracing::warn!(%error, token_id = token.id(), "failed to update owner index");
        }
    }

    pub async fn sync_owner_index_mutation(&self, mutation: &TokenMutation) {
        let result = match mutation {
            TokenMutation::Create(token) => self.store.index_owner_token(token).await,
            TokenMutation::Update { expected, token } => {
                if expected.owner() != token.owner()
                    && let Err(error) = self
                        .store
                        .remove_owner_index_token(expected.owner(), expected.id())
                        .await
                {
                    tracing::warn!(%error, token_id = expected.id(), "failed to update owner index");
                }
                self.store.index_owner_token(token).await
            }
            TokenMutation::Delete { expected } => {
                self.store
                    .remove_owner_index_token(expected.owner(), expected.id())
                    .await
            }
        };
        if let Err(error) = result {
            tracing::warn!(%error, "failed to update owner index");
        }
    }
}
impl AuthService {
    pub async fn select_token_from_scan(
        &self,
        owner: &str,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> PlatformResult<ApiToken> {
        let tokens = self.scan_tokens_for_owner(owner).await?;
        self.repair_owner_index(owner, &tokens).await;
        tokens
            .into_iter()
            .filter(|token| {
                token.is_active(now) && scope.is_none_or(|scope| token.metadata().has_scope(scope))
            })
            .min_by(|left, right| {
                OwnerIndexEntry::compare(
                    &OwnerIndexEntry::from_token(left),
                    &OwnerIndexEntry::from_token(right),
                )
            })
            .ok_or_else(|| Self::no_active_token(scope))
    }

    pub async fn tokens_from_owner_scan(&self, owner: &str) -> PlatformResult<Vec<ApiToken>> {
        let mut tokens = self.scan_tokens_for_owner(owner).await?;
        tokens.sort_by(|left, right| left.id().cmp(right.id()));
        self.repair_owner_index(owner, &tokens).await;
        Ok(tokens)
    }

    async fn scan_tokens_for_owner(&self, owner: &str) -> PlatformResult<Vec<ApiToken>> {
        let mut ids = self.store.ids().await?;
        ids.sort();
        ids.dedup();
        let mut tokens = Vec::new();
        for id in ids {
            if let Some(token) = self.stored_token(&id).await?
                && token.owner() == owner
            {
                tokens.push(token);
            }
        }
        Ok(tokens)
    }

    async fn repair_owner_index(&self, owner: &str, tokens: &[ApiToken]) {
        let index = OwnerIndex::from_tokens(owner, tokens);
        if let Err(error) = self.store.put_owner_index(&index).await {
            tracing::warn!(%error, %owner, "failed to repair owner index");
        }
    }

    pub fn no_active_token(scope: Option<TokenScope>) -> PlatformError {
        match scope {
            Some(scope) => PlatformError::auth_invalid_field(
                "scope",
                format!("owner has no active API token with required scope {scope:?}"),
            ),
            None => PlatformError::auth_invalid_field("owner", "has no active API token"),
        }
    }

    pub fn owner_index_error(detail: String) -> PlatformError {
        PlatformError::auth_store("read", "owner-index", false, detail)
    }
}
