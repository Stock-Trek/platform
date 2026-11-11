use crate::{ApiToken, AuthService, OwnerIndex, OwnerIndexEntry, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::BpResult;

impl AuthService {
    pub(crate) async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        let owner = ApiToken::validated_owner(owner)?;
        let Some(index) = self.store.owner_index(owner).await? else {
            return self.tokens_from_owner_scan(owner).await;
        };
        if index.owner() != owner {
            return Err(Self::owner_index_error(format!(
                "owner index for {owner:?} belongs to {:?}",
                index.owner()
            )));
        }
        match self.tokens_from_owner_index(owner, &index).await {
            Ok(tokens) => Ok(tokens),
            Err(error) => {
                tracing::warn!(%error, %owner, "owner index is stale; rebuilding from tokens");
                self.tokens_from_owner_scan(owner).await
            }
        }
    }

    pub(crate) async fn active_tokens_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> BpResult<Vec<ApiToken>> {
        Ok(self
            .tokens_for_owner(owner)
            .await?
            .into_iter()
            .filter(|token| token.is_active(now))
            .collect())
    }

    pub(crate) async fn token_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.select_token_for_owner(owner, None, now).await
    }

    pub(crate) async fn token_for_owner_with_scope(
        &self,
        owner: &str,
        scope: TokenScope,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.select_token_for_owner(owner, Some(scope), now).await
    }

    async fn select_token_for_owner(
        &self,
        owner: &str,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let owner = ApiToken::validated_owner(owner)?;
        let Some(index) = self.store.owner_index(owner).await? else {
            return self.select_token_from_scan(owner, scope, now).await;
        };
        if index.owner() != owner {
            return Err(Self::owner_index_error(format!(
                "owner index for {owner:?} belongs to {:?}",
                index.owner()
            )));
        }
        let candidate = index
            .tokens()
            .iter()
            .filter(|entry| {
                entry.owner() == owner
                    && entry.is_active(now)
                    && scope.is_none_or(|scope| entry.has_scope(scope))
            })
            .min_by(|left, right| OwnerIndexEntry::compare(left, right));
        let Some(candidate) = candidate else {
            if index.is_empty() {
                return self.select_token_from_scan(owner, scope, now).await;
            }
            return Err(Self::no_active_token(scope));
        };
        let Some(token) = self.stored_token(candidate.id()).await? else {
            tracing::warn!(owner = %owner, token_id = candidate.id(), "owner index references a missing token; rescanning");
            return self.select_token_from_scan(owner, scope, now).await;
        };
        if token.owner() != owner
            || !candidate.matches_token(&token)
            || !token.is_active(now)
            || !scope.is_none_or(|scope| token.metadata().has_scope(scope))
        {
            tracing::warn!(owner = %owner, token_id = candidate.id(), "owner index entry is stale; rescanning");
            return self.select_token_from_scan(owner, scope, now).await;
        }
        Ok(token)
    }

    async fn tokens_from_owner_index(
        &self,
        owner: &str,
        index: &OwnerIndex,
    ) -> BpResult<Vec<ApiToken>> {
        let mut ids = Vec::with_capacity(index.tokens().len());
        for entry in index.tokens() {
            if entry.owner() != owner || ids.contains(&entry.id().to_string()) {
                return Err(Self::owner_index_error(format!(
                    "owner index for {owner:?} contains an invalid entry"
                )));
            }
            ids.push(entry.id().to_string());
        }
        let mut tokens = Vec::with_capacity(ids.len());
        for (id, entry) in ids.iter().zip(index.tokens()) {
            let Some(token) = self.stored_token(id).await? else {
                return Err(Self::owner_index_error(format!(
                    "owner index references missing token {id}"
                )));
            };
            if token.owner() != owner || !entry.matches_token(&token) {
                return Err(Self::owner_index_error(format!(
                    "owner index entry for {id} is stale"
                )));
            }
            tokens.push(token);
        }
        tokens.sort_by(|left, right| left.id().cmp(right.id()));
        Ok(tokens)
    }
}
