use crate::{
    NatsOwnershipSource, NoopPortfolioFeed, OrderRequestDelivery, OrderRequestServer,
    OrderRequestService, OwnershipConsumer, PortfolioAuthService, PortfolioConfig,
    PortfolioService,
};
use platform_errors::BpResult;
use platform_runtime::{Runtime, Service};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-portfolio-service");

pub struct PortfolioRunner;

impl PortfolioRunner {
    pub async fn run(config: PortfolioConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let auth_service = PortfolioAuthService::connect(config.auth().clone()).await?;
        let portfolio = Arc::new(PortfolioService::new(Arc::new(NoopPortfolioFeed::new())));
        let source = Arc::new(NatsOwnershipSource::connect(config.ownership()).await?);
        let consumer = OwnershipConsumer::new(
            portfolio.clone(),
            source,
            config.ownership().retry_backoff(),
        );
        let service = Arc::new(
            OrderRequestService::with_jwks_url(auth_service, config.auth().jwks_url())?
                .with_portfolio_service(portfolio),
        );
        let delivery = Arc::new(OrderRequestDelivery::new(
            config.order_request().delivery_url(),
        )?);
        let server =
            OrderRequestServer::bind(config.order_request().bind_address(), service, delivery)
                .await?;
        tokio::try_join!(
            server.serve_with_shutdown(Runtime::shutdown_signal()),
            consumer.run(Runtime::shutdown_signal())
        )?;
        Ok(())
    }
}
