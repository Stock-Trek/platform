use crate::{ApiToken, AuthService, OwnerIndex, OwnerIndexEntry, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};

impl AuthService {
    pub(crate) async fn select_token_from_scan(
        &self,
        owner: &str,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BuildResult<ApiToken> {
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

    pub(crate) async fn tokens_from_owner_scan(&self, owner: &str) -> BuildResult<Vec<ApiToken>> {
        let mut tokens = self.scan_tokens_for_owner(owner).await?;
        tokens.sort_by(|left, right| left.id().cmp(right.id()));
        self.repair_owner_index(owner, &tokens).await;
        Ok(tokens)
    }

    async fn scan_tokens_for_owner(&self, owner: &str) -> BuildResult<Vec<ApiToken>> {
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

    pub(crate) fn no_active_token(scope: Option<TokenScope>) -> BuildError {
        match scope {
            Some(scope) => BuildError::auth_invalid_field(
                "scope",
                format!("owner has no active API token with required scope {scope:?}"),
            ),
            None => BuildError::auth_invalid_field("owner", "has no active API token"),
        }
    }

    pub(crate) fn owner_index_error(detail: String) -> BuildError {
        BuildError::auth_store("read", "owner-index", false, detail)
    }
}
