use crate::connector::portfolio::ConnectorPortfolioUpdate;
use async_trait::async_trait;
use platform_errors::BuildResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioUpdateOutcome {
    Applied,
    IgnoredUntracked,
    IgnoredStale,
}

#[async_trait]
pub trait PortfolioUpdateSink: Send + Sync {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> BuildResult<PortfolioUpdateOutcome>;
}
