use crate::{
    api_key_credential::ApiKeyCredentials, rate_limited::RateLimit, signer::Signer, urls::Urls,
};
use platform_errors::PlatformResult;
use platform_types::UsageCount;
use std::collections::HashMap;
use stock_trek_types::cex::cex_id::CexId;

pub trait ETExchange {
    type Credentials;

    fn cex_id(&self) -> CexId;
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount>;
    fn signer(credentials: &ApiKeyCredentials) -> PlatformResult<Signer>;
    fn urls(&self) -> &impl Urls;
}
