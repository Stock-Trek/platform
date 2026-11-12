use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_types::portfolio::AccountEvent;

#[async_trait]
pub trait AccountEventStream: Send {
    async fn next(&mut self) -> BpResult<Option<AccountEvent>>;
}
