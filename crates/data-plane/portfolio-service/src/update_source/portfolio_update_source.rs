use crate::update_source::PortfolioUpdateMessage;
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait PortfolioUpdateSource: Send + Sync {
    async fn next(&self) -> PlatformResult<Option<PortfolioUpdateMessage>>;

    async fn checkpoint(&self, sequence: u64) -> PlatformResult<()>;

    async fn acknowledge(&self, message: &PortfolioUpdateMessage) -> PlatformResult<()>;
}
