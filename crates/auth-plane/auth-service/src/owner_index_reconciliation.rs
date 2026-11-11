use crate::{ApiToken, AuthService, OwnerIndex, TokenMutation};
use platform_errors::BpResult;
use std::collections::BTreeMap;

impl AuthService {
    pub(crate) async fn reconcile_owner_indexes(&self) -> BpResult<usize> {
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

    pub(crate) async fn sync_owner_index_token(&self, token: &ApiToken) {
        if let Err(error) = self.store.index_owner_token(token).await {
            tracing::warn!(%error, token_id = token.id(), "failed to update owner index");
        }
    }

    pub(crate) async fn sync_owner_index_mutation(&self, mutation: &TokenMutation) {
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
