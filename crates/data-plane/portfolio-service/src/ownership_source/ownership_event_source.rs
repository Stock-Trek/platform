use crate::ownership_source::OwnershipMessage;
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait OwnershipEventSource: Send + Sync {
    async fn next(&self) -> PlatformResult<Option<OwnershipMessage>>;

    async fn checkpoint(&self, sequence: u64) -> PlatformResult<()>;

    async fn acknowledge(&self, message: &OwnershipMessage) -> PlatformResult<()>;
}
