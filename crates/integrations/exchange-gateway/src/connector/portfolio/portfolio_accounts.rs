use crate::connector::portfolio::PortfolioCredentialSource;
use async_trait::async_trait;
use parking_lot::RwLock;
use platform_errors::{BuildError, BuildResult};
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::collections::{BTreeMap, BTreeSet};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct PortfolioAccounts {
    credentials: RwLock<BTreeMap<(CexId, AccountId), ApiKeyCredentials>>,
}

impl PortfolioAccounts {
    pub fn new(entries: impl IntoIterator<Item = ((CexId, AccountId), ApiKeyCredentials)>) -> Self {
        Self {
            credentials: RwLock::new(entries.into_iter().collect()),
        }
    }

    pub fn insert(
        &self,
        cex_id: CexId,
        account_id: AccountId,
        credentials: ApiKeyCredentials,
    ) -> Option<ApiKeyCredentials> {
        self.credentials
            .write()
            .insert((cex_id, account_id), credentials)
    }

    pub fn remove(&self, cex_id: CexId, account_id: &AccountId) -> Option<ApiKeyCredentials> {
        self.credentials
            .write()
            .remove(&(cex_id, account_id.clone()))
    }

    pub fn replace(
        &self,
        cex_id: CexId,
        entries: impl IntoIterator<Item = (AccountId, ApiKeyCredentials)>,
    ) {
        let mut credentials = self.credentials.write();
        credentials.retain(|(existing_cex, _), _| *existing_cex != cex_id);
        credentials.extend(
            entries
                .into_iter()
                .map(|(account_id, credentials)| ((cex_id, account_id), credentials)),
        );
    }

    pub fn accounts(&self, cex_id: CexId) -> BTreeSet<AccountId> {
        self.credentials
            .read()
            .keys()
            .filter(|(existing_cex, _)| *existing_cex == cex_id)
            .map(|(_, account_id)| account_id.clone())
            .collect()
    }
}

#[async_trait]
impl PortfolioCredentialSource for PortfolioAccounts {
    async fn credentials(
        &self,
        cex_id: CexId,
        account_id: &AccountId,
    ) -> BuildResult<ApiKeyCredentials> {
        self.credentials
            .read()
            .get(&(cex_id, account_id.clone()))
            .cloned()
            .ok_or_else(|| {
                BuildError::bad_value(
                    "portfolio account",
                    format!("{cex_id}:{}", account_id.0),
                    "has no configured credentials",
                )
            })
    }
}
