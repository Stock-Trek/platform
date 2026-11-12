use crate::{
    BinancePortfolioConnector, CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT,
    GatewayBinanceTransport, GroupingPortfolioFeed, NatsOwnershipSource, NatsUpdateSource,
    NoopPortfolioFeed, OrderRequestDelivery, OrderRequestServer, OrderRequestService,
    OwnershipConsumer, PORTFOLIO_UPDATE_STREAM_PREFIX, PortfolioAuthService, PortfolioConfig,
    PortfolioFeed, PortfolioService, PortfolioUpdateConsumer, PublishingPortfolioUpdateSink,
    StaticBinanceCredentials,
};
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use platform_nats::{EventPublisher, NatsPublisher};
use platform_runtime::{Runtime, Service};
use secrecy::{ExposeSecret, SecretString};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-portfolio-service");

pub struct PortfolioRunner;

impl PortfolioRunner {
    pub async fn run(config: PortfolioConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let auth_service = PortfolioAuthService::connect(config.auth().clone()).await?;
        let publisher: Arc<dyn EventPublisher> = Arc::new(Self::publisher(&config).await?);
        let feed: Arc<dyn PortfolioFeed> = if config.binance().enabled() {
            let credentials = config
                .binance()
                .credentials()
                .iter()
                .map(|credential| {
                    (
                        credential.account().to_string(),
                        ApiKeyCredentials {
                            api_key: credential.api_key().to_string(),
                            secret: SecretString::from(
                                credential.secret().expose_secret().to_string(),
                            ),
                        },
                    )
                })
                .collect::<Vec<_>>();
            let transport = Arc::new(GatewayBinanceTransport::new(
                config.binance().rest_url(),
                config.binance().websocket_url(),
            )?);
            let connector = Arc::new(
                BinancePortfolioConnector::new(
                    transport,
                    Arc::new(StaticBinanceCredentials::new(credentials)),
                    config.binance().listen_key_refresh(),
                    config.binance().reconnect_backoff(),
                )
                .with_health_publisher(publisher.clone(), CONNECTOR_HEALTH_SUBJECT),
            );
            Arc::new(GroupingPortfolioFeed::new(connector))
        } else {
            Arc::new(NoopPortfolioFeed::new())
        };
        let portfolio = Arc::new(PortfolioService::with_freshness_policy(
            feed.clone(),
            config.freshness().policy(),
        ));
        let sink = Arc::new(PublishingPortfolioUpdateSink::new(
            publisher,
            portfolio.clone(),
        ));
        feed.set_update_sink(sink)?;
        let ownership_source = Arc::new(NatsOwnershipSource::connect(config.ownership()).await?);
        let update_source = Arc::new(NatsUpdateSource::connect(config.update()).await?);
        let ownership_consumer = OwnershipConsumer::new(
            portfolio.clone(),
            ownership_source,
            config.ownership().retry_backoff(),
        );
        let update_consumer = PortfolioUpdateConsumer::new(
            portfolio.clone(),
            update_source,
            config.update().retry_backoff(),
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
            ownership_consumer.run(Runtime::shutdown_signal()),
            update_consumer.run(Runtime::shutdown_signal())
        )?;
        Ok(())
    }

    async fn publisher(config: &PortfolioConfig) -> BpResult<NatsPublisher> {
        let publisher = NatsPublisher::connect(
            config.update().nats_url(),
            &format!("{}-publisher", config.update().client_name()),
        )
        .await?;
        publisher
            .ensure_stream(PORTFOLIO_UPDATE_STREAM_PREFIX)
            .await?;
        publisher
            .ensure_stream(CONNECTOR_HEALTH_STREAM_PREFIX)
            .await?;
        Ok(publisher)
    }
}
