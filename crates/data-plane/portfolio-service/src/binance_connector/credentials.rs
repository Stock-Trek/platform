use crate::AccountKey;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::collections::BTreeMap;

#[async_trait]
pub trait BinanceCredentialSource: Send + Sync {
    async fn credentials(&self, account: &AccountKey) -> BpResult<ApiKeyCredentials>;
}

pub struct StaticBinanceCredentials {
    credentials: BTreeMap<String, ApiKeyCredentials>,
}

impl StaticBinanceCredentials {
    pub fn new(entries: impl IntoIterator<Item = (String, ApiKeyCredentials)>) -> Self {
        Self {
            credentials: entries.into_iter().collect(),
        }
    }
}

#[async_trait]
impl BinanceCredentialSource for StaticBinanceCredentials {
    async fn credentials(&self, account: &AccountKey) -> BpResult<ApiKeyCredentials> {
        self.credentials
            .get(&account.account_id().0)
            .cloned()
            .ok_or_else(|| {
                platform_errors::BpError::bad_value(
                    "binance account",
                    &account.account_id().0,
                    "has no configured credentials",
                )
            })
    }
}
