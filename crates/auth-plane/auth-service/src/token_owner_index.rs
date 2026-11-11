use crate::{ApiToken, AuthService, OwnerIndex, OwnerIndexEntry, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub(crate) async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        let owner = ApiToken::validated_owner(owner)?;
        let Some(index) = self.store.owner_index(owner).await? else {
            return Ok(Vec::new());
        };
        if index.owner() != owner {
            return Err(Self::owner_index_error(format!(
                "owner index for {owner:?} belongs to {:?}",
                index.owner()
            )));
        }
        self.tokens_from_owner_index(owner, &index).await
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
            return Err(Self::no_active_token(scope));
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
            return Err(Self::no_active_token(scope));
        };
        let token = self
            .stored_token(candidate.id())
            .await?
            .ok_or_else(|| Self::owner_index_error(format!("missing token {}", candidate.id())))?;
        if token.owner() != owner
            || !candidate.matches_token(&token)
            || !token.is_active(now)
            || !scope.is_none_or(|scope| token.metadata().has_scope(scope))
        {
            return Err(Self::owner_index_error(format!(
                "owner index entry for {} is stale",
                candidate.id()
            )));
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

    fn no_active_token(scope: Option<TokenScope>) -> BpError {
        match scope {
            Some(scope) => BpError::auth_invalid_field(
                "scope",
                format!("owner has no active API token with required scope {scope:?}"),
            ),
            None => BpError::auth_invalid_field("owner", "has no active API token"),
        }
    }

    fn owner_index_error(detail: String) -> BpError {
        BpError::auth_store("read", "owner-index", false, detail)
    }
}
