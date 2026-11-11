use crate::{
    OrderRequestDelivery, OrderRequestServer, OrderRequestService, PortfolioAuthService,
    PortfolioConfig,
};
use platform_errors::{BpError, BpResult};
use platform_runtime::Service;
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-portfolio-service");

pub struct PortfolioRunner;

impl PortfolioRunner {
    pub async fn run(config: PortfolioConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let auth_service = PortfolioAuthService::connect(config.auth().clone()).await?;
        let service = Arc::new(OrderRequestService::with_jwks_url(
            auth_service,
            config.auth().jwks_url(),
        )?);
        let delivery = Arc::new(OrderRequestDelivery::new(
            config.order_request().delivery_url(),
        )?);
        let server =
            OrderRequestServer::bind(config.order_request().bind_address(), service, delivery)
                .await?;
        tokio::select! {
            result = server.serve() => result,
            result = tokio::signal::ctrl_c() => result.map_err(BpError::external),
        }
    }
}
