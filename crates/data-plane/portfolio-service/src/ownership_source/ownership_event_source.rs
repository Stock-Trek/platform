use super::OwnershipMessage;
use async_trait::async_trait;
use platform_errors::BuildResult;

#[async_trait]
pub trait OwnershipEventSource: Send + Sync {
    async fn next(&self) -> BuildResult<Option<OwnershipMessage>>;

    async fn checkpoint(&self, sequence: u64) -> BuildResult<()>;

    async fn acknowledge(&self, message: &OwnershipMessage) -> BuildResult<()>;
}
