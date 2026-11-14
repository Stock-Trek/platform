use crate::{AccountMapping, OwnershipCommand, OwnershipCommit, OwnershipStore};
use chrono::{DateTime, Utc};
use platform_auth_service::ApiToken;
use platform_errors::{PlatformError, PlatformResult};
use platform_portfolio_ownership::{AccountKey, OwnershipAction};
use std::sync::Arc;

pub struct RegistryService {
    store: Arc<dyn OwnershipStore>,
}

impl RegistryService {
    pub fn new(store: Arc<dyn OwnershipStore>) -> Self {
        Self { store }
    }

    pub async fn add(
        &self,
        owner: &str,
        cex: &str,
        account_id: &str,
        at: DateTime<Utc>,
    ) -> PlatformResult<AccountMapping> {
        self.commit(owner, cex, account_id, OwnershipAction::Add, at)
            .await
    }

    pub async fn remove(
        &self,
        owner: &str,
        cex: &str,
        account_id: &str,
        at: DateTime<Utc>,
    ) -> PlatformResult<AccountMapping> {
        self.commit(owner, cex, account_id, OwnershipAction::Remove, at)
            .await
    }

    pub async fn accounts(&self, owner: &str) -> PlatformResult<Vec<AccountMapping>> {
        ApiToken::validated_owner(owner)?;
        self.store.owner_accounts(owner).await
    }

    async fn commit(
        &self,
        owner: &str,
        cex: &str,
        account_id: &str,
        action: OwnershipAction,
        at: DateTime<Utc>,
    ) -> PlatformResult<AccountMapping> {
        let owner = ApiToken::validated_owner(owner)?;
        let account = AccountKey::parse(cex, account_id)?;
        let command = OwnershipCommand::new(owner, account.clone(), action, at);
        match self.store.commit(command).await? {
            OwnershipCommit::Applied(_) | OwnershipCommit::Unchanged(_) => self
                .store
                .get(&account)
                .await?
                .map(|record| record.mapping())
                .ok_or_else(|| {
                    PlatformError::external(std::io::Error::other("mapping disappeared"))
                }),
        }
    }
}
