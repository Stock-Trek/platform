use super::PortfolioCredentialSource;
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult};
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::collections::BTreeMap;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct StaticPortfolioCredentials {
    credentials: BTreeMap<(CexId, AccountId), ApiKeyCredentials>,
}

impl StaticPortfolioCredentials {
    pub fn new(entries: impl IntoIterator<Item = ((CexId, AccountId), ApiKeyCredentials)>) -> Self {
        Self {
            credentials: entries.into_iter().collect(),
        }
    }
}

#[async_trait]
impl PortfolioCredentialSource for StaticPortfolioCredentials {
    async fn credentials(
        &self,
        cex_id: CexId,
        account_id: &AccountId,
    ) -> BuildResult<ApiKeyCredentials> {
        self.credentials
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
