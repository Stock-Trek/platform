use super::PortfolioUpdateMessage;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait PortfolioUpdateSource: Send + Sync {
    async fn next(&self) -> BpResult<Option<PortfolioUpdateMessage>>;

    async fn checkpoint(&self, sequence: u64) -> BpResult<()>;

    async fn acknowledge(&self, message: &PortfolioUpdateMessage) -> BpResult<()>;
}
