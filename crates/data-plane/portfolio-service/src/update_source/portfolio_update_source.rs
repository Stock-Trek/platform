use super::PortfolioUpdateMessage;
use async_trait::async_trait;
use platform_errors::BuildResult;

#[async_trait]
pub trait PortfolioUpdateSource: Send + Sync {
    async fn next(&self) -> BuildResult<Option<PortfolioUpdateMessage>>;

    async fn checkpoint(&self, sequence: u64) -> BuildResult<()>;

    async fn acknowledge(&self, message: &PortfolioUpdateMessage) -> BuildResult<()>;
}
