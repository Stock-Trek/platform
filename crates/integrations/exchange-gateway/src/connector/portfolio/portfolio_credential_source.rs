use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[async_trait]
pub trait PortfolioCredentialSource: Send + Sync {
    async fn credentials(
        &self,
        cex_id: CexId,
        account_id: &AccountId,
    ) -> BpResult<ApiKeyCredentials>;
}
