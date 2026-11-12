use super::{PortfolioConnection, PortfolioUpdateSink};
use async_trait::async_trait;
use platform_errors::BpResult;
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;

#[async_trait]
pub trait PortfolioConnector: Send + Sync {
    async fn connect(
        &self,
        cex_id: CexId,
        sink: Arc<dyn PortfolioUpdateSink>,
    ) -> BpResult<Arc<dyn PortfolioConnection>>;
}
