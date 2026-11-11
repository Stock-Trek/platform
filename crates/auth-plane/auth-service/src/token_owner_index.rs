use crate::{ApiToken, AuthService, OwnerIndex, OwnerIndexEntry, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub(crate) async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        let owner = ApiToken::validated_owner(owner)?;
        match self.store.owner_index(owner).await {
            Ok(Some(index)) if index.owner() == owner => {
                if let Some(tokens) = self.tokens_from_owner_index(owner, &index).await? {
                    return Ok(tokens);
                }
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, owner, "failed to read owner index");
            }
        }
        let tokens = self.scan_tokens_for_owner(owner).await?;
        if let Err(error) = self.rebuild_owner_index(owner, &tokens).await {
            tracing::warn!(%error, owner, "failed to rebuild owner index");
        }
        Ok(tokens)
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
        if let Ok(Some(index)) = self.store.owner_index(owner).await
            && index.owner() == owner
        {
            let candidate = index
                .tokens()
                .iter()
                .filter(|entry| {
                    entry.owner() == owner
                        && entry.is_active(now)
                        && scope.is_none_or(|scope| entry.has_scope(scope))
                })
                .min_by(|left, right| OwnerIndexEntry::compare(left, right));
            if let Some(candidate) = candidate
                && let Some(token) = self.stored_token(candidate.id()).await?
                && token.owner() == owner
                && candidate.matches_token(&token)
                && token.is_active(now)
                && scope.is_none_or(|scope| token.metadata().has_scope(scope))
            {
                return Ok(token);
            }
            if !index.is_empty() {
                let mut current = true;
                for entry in index.tokens() {
                    match self.stored_token(entry.id()).await? {
                        Some(token) if token.owner() == owner && entry.matches_token(&token) => {}
                        _ => {
                            current = false;
                            break;
                        }
                    }
                }
                if current {
                    return Err(Self::no_active_token(scope));
                }
            }
        }
        let tokens = self.scan_tokens_for_owner(owner).await?;
        if let Err(error) = self.rebuild_owner_index(owner, &tokens).await {
            tracing::warn!(%error, owner, "failed to rebuild owner index");
        }
        Self::select_from_tokens(tokens, scope, now)
    }

    async fn tokens_from_owner_index(
        &self,
        owner: &str,
        index: &OwnerIndex,
    ) -> BpResult<Option<Vec<ApiToken>>> {
        let mut ids = Vec::with_capacity(index.tokens().len());
        for entry in index.tokens() {
            if entry.owner() != owner || ids.contains(&entry.id().to_string()) {
                return Ok(None);
            }
            ids.push(entry.id().to_string());
        }
        let mut tokens = Vec::with_capacity(ids.len());
        for (id, entry) in ids.iter().zip(index.tokens()) {
            let Some(token) = self.stored_token(id).await? else {
                return Ok(None);
            };
            if token.owner() != owner || !entry.matches_token(&token) {
                return Ok(None);
            }
            tokens.push(token);
        }
        tokens.sort_by(|left, right| left.id().cmp(right.id()));
        Ok(Some(tokens))
    }

    pub(crate) async fn scan_tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
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

    pub(crate) async fn rebuild_owner_index(
        &self,
        owner: &str,
        tokens: &[ApiToken],
    ) -> BpResult<()> {
        self.store
            .put_owner_index(&OwnerIndex::from_tokens(owner, tokens))
            .await
    }

    fn select_from_tokens(
        tokens: Vec<ApiToken>,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let active: Vec<_> = tokens
            .into_iter()
            .filter(|token| token.is_active(now))
            .collect();
        if active.is_empty() {
            return Err(Self::no_active_token(scope));
        }
        match scope {
            Some(scope) => active
                .into_iter()
                .filter(|token| token.metadata().has_scope(scope))
                .min_by(Self::compare_tokens)
                .ok_or_else(|| Self::no_active_token(Some(scope))),
            None => active
                .into_iter()
                .min_by(Self::compare_tokens)
                .ok_or_else(|| Self::no_active_token(None)),
        }
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

    fn compare_tokens(left: &ApiToken, right: &ApiToken) -> std::cmp::Ordering {
        left.metadata()
            .replaced_by()
            .is_some()
            .cmp(&right.metadata().replaced_by().is_some())
            .then_with(|| {
                right
                    .metadata()
                    .last_used_at()
                    .cmp(&left.metadata().last_used_at())
            })
            .then_with(|| right.created_at().cmp(&left.created_at()))
            .then_with(|| left.id().cmp(right.id()))
    }
}
