use crate::{
    NatsConnectorHealthSink, NatsOwnershipSource, NatsUpdateSource, OrderRequestDelivery,
    OrderRequestServer, OrderRequestService, OwnershipConsumer, PORTFOLIO_UPDATE_STREAM_PREFIX,
    PortfolioAuthService, PortfolioConfig, PortfolioService, PortfolioUpdateConsumer,
    PublishingPortfolioUpdateSink, read_model::PortfolioReadModelStore,
};
use platform_errors::BuildResult;
use platform_exchange_gateway::{CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT};
use platform_nats::{EventPublisher, NatsPublisher};
use platform_portfolio_ownership::OWNERSHIP_STREAM_PREFIX;
use platform_runtime::{Runtime, Service};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-portfolio-service");

pub struct PortfolioRunner;

impl PortfolioRunner {
    pub async fn run(config: PortfolioConfig) -> BuildResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let _metrics = SERVICE.init_metrics("platform/portfolio-service")?;
        let auth_service = PortfolioAuthService::connect(config.auth().clone()).await?;
        let publisher: Arc<dyn EventPublisher> = Arc::new(Self::publisher(&config).await?);
        let store = Arc::new(
            PortfolioReadModelStore::connect(
                config.ownership().nats_url(),
                &format!("{}-read-model", config.ownership().client_name()),
                config.ownership().checkpoint_bucket(),
            )
            .await?,
        );
        let read_model = store.load().await?;
        let feed = config
            .connector()
            .feed(Arc::new(NatsConnectorHealthSink::new(
                Arc::clone(&publisher),
                CONNECTOR_HEALTH_SUBJECT,
            )));
        let portfolio = Arc::new(PortfolioService::with_freshness_policy(
            Arc::clone(&feed),
            config.freshness().policy(),
        ));
        portfolio.restore(read_model)?;
        let sink = Arc::new(PublishingPortfolioUpdateSink::new(
            Arc::clone(&publisher),
            Arc::clone(&portfolio),
        ));
        feed.set_update_sink(sink);
        portfolio.sync_feed().await?;
        let ownership_source = Arc::new(NatsOwnershipSource::connect(config.ownership()).await?);
        let ownership_consumer = OwnershipConsumer::new(
            Arc::clone(&portfolio),
            ownership_source,
            config.ownership().retry_backoff(),
        )
        .with_read_model_store(Arc::clone(&store));
        ownership_consumer.run_once().await?;
        let update_source = Arc::new(NatsUpdateSource::connect(config.update()).await?);
        let update_consumer = PortfolioUpdateConsumer::new(
            Arc::clone(&portfolio),
            update_source,
            config.update().retry_backoff(),
        )
        .with_read_model_store(store);
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
            ownership_consumer.run(Runtime::shutdown_signal()),
            update_consumer.run(Runtime::shutdown_signal())
        )?;
        Ok(())
    }

    async fn publisher(config: &PortfolioConfig) -> BuildResult<NatsPublisher> {
        let publisher = NatsPublisher::connect(
            config.update().nats_url(),
            &format!("{}-publisher", config.update().client_name()),
        )
        .await?;
        publisher
            .ensure_stream_spec(
                &config
                    .update()
                    .stream()
                    .spec(PORTFOLIO_UPDATE_STREAM_PREFIX),
            )
            .await?;
        publisher
            .ensure_stream_spec(&config.ownership().stream().spec(OWNERSHIP_STREAM_PREFIX))
            .await?;
        publisher
            .ensure_stream(CONNECTOR_HEALTH_STREAM_PREFIX)
            .await?;
        Ok(publisher)
    }
}
