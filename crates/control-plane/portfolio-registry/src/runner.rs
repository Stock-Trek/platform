use crate::{
    AdminApi, ApiServer, DynamoDbOwnershipStore, OwnershipOutboxRelay, OwnershipStore,
    PortfolioRegistryConfig, ReadApi, RegistryService,
};
use platform_auth_service::JwksCache;
use platform_aws::Aws;
use platform_errors::BpResult;
use platform_nats::{EventPublisher, NatsPublisher};
use platform_portfolio_ownership::OWNERSHIP_STREAM_PREFIX;
use platform_runtime::{Runtime, Service};
use std::sync::Arc;

const SERVICE: Service = Service::new("platform-portfolio-registry");

pub struct RegistryRunner;

impl RegistryRunner {
    pub async fn run(config: PortfolioRegistryConfig) -> BpResult<()> {
        SERVICE.init_logging(config.log_filter())?;
        let _metrics = SERVICE.init_metrics("platform/portfolio-registry")?;
        let sdk_config = Aws::load_default_sdk_config().await;
        let store: Arc<dyn OwnershipStore> = Arc::new(DynamoDbOwnershipStore::new(
            Aws::dynamodb_client(&sdk_config),
            config.store().table(),
        ));
        let publisher =
            NatsPublisher::connect(config.nats().url(), config.nats().client_name()).await?;
        publisher
            .ensure_stream_spec(&config.nats().stream().spec(OWNERSHIP_STREAM_PREFIX))
            .await?;
        let publisher: Arc<dyn EventPublisher> = Arc::new(publisher);
        let service = Arc::new(RegistryService::new(store.clone()));
        let relay = OwnershipOutboxRelay::new(store, publisher, config.nats().outbox_interval())?;
        let admin = AdminApi::new(service.clone(), config.admin().token());
        let read = ReadApi::new(service, JwksCache::new(config.read().jwks_url())?);
        let admin_server = ApiServer::bind(config.admin().bind_address(), Arc::new(admin)).await?;
        let read_server = ApiServer::bind(config.read().bind_address(), Arc::new(read)).await?;
        tokio::try_join!(
            admin_server.serve(),
            read_server.serve(),
            relay.run(Runtime::shutdown_signal())
        )?;
        Ok(())
    }
}
