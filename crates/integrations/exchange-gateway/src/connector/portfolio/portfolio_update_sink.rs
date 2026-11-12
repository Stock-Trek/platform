use super::{ConnectorPortfolioUpdate, PortfolioUpdateApply};
use async_trait::async_trait;
use platform_errors::BuildResult;

#[async_trait]
pub trait PortfolioUpdateSink: Send + Sync {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> BuildResult<PortfolioUpdateApply>;
}
