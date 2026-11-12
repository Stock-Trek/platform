use super::AccountEvent;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait AccountEventStream: Send {
    async fn next(&mut self) -> BpResult<Option<AccountEvent>>;
}
