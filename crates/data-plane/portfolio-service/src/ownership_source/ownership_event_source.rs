use super::OwnershipMessage;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait OwnershipEventSource: Send + Sync {
    async fn next(&self) -> BpResult<Option<OwnershipMessage>>;

    async fn checkpoint(&self, sequence: u64) -> BpResult<()>;

    async fn acknowledge(&self, message: &OwnershipMessage) -> BpResult<()>;
}
