use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::collections::BTreeMap;
use stock_trek_types::cex::account_id::AccountId;

#[async_trait]
pub trait BinanceCredentialSource: Send + Sync {
    async fn credentials(&self, account_id: &AccountId) -> BpResult<ApiKeyCredentials>;
}

pub struct StaticBinanceCredentials {
    credentials: BTreeMap<AccountId, ApiKeyCredentials>,
}

impl StaticBinanceCredentials {
    pub fn new(entries: impl IntoIterator<Item = (AccountId, ApiKeyCredentials)>) -> Self {
        Self {
            credentials: entries.into_iter().collect(),
        }
    }
}

#[async_trait]
impl BinanceCredentialSource for StaticBinanceCredentials {
    async fn credentials(&self, account_id: &AccountId) -> BpResult<ApiKeyCredentials> {
        self.credentials.get(account_id).cloned().ok_or_else(|| {
            BpError::bad_value(
                "binance account",
                &account_id.0,
                "has no configured credentials",
            )
        })
    }
}
