mod auth_service;
mod binance_connector;
mod config;
mod feed;
mod freshness;
mod health;
mod nats_ownership_source;
mod nats_update_source;
mod order_delivery;
mod order_request;
mod order_request_connection;
mod order_request_error;
mod order_request_server;
mod ownership_consumer;
mod ownership_registry;
mod ownership_source;
mod portfolio;
mod portfolio_codec;
mod publishing_sink;
mod runner;
mod service;
mod state;
mod update_consumer;
mod update_source;

pub use auth_service::PortfolioAuthService;
pub use binance_connector::{
    AccountPositionEvent, AccountSnapshot, BinanceAccountEvent, BinanceCredentialSource,
    BinanceEventStream, BinancePortfolioConnector, BinanceTransport, ReqwestBinanceTransport,
    StaticBinanceCredentials,
};
pub use config::{
    AuthConfig, BinanceConfig, BinanceCredentialConfig, FreshnessConfig, OrderRequestConfig,
    OwnershipConfig, PortfolioConfig, UpdateConfig,
};
pub use feed::{
    GroupingPortfolioFeed, NoopPortfolioFeed, PortfolioConnection, PortfolioConnector,
    PortfolioFeed, PortfolioUpdateApply, PortfolioUpdateSink,
};
pub use freshness::{FreshnessPolicy, PortfolioFreshness, StaleOrderPolicy};
pub use health::{
    CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorState,
};
pub use nats_ownership_source::NatsOwnershipSource;
pub use nats_update_source::NatsUpdateSource;
pub use order_delivery::OrderRequestDelivery;
pub use order_request::{OrderRequestService, SignedOrderSubmission};
pub use order_request_error::OrderRequestError;
pub use order_request_server::OrderRequestServer;
pub use ownership_consumer::OwnershipConsumer;
pub use ownership_registry::{OwnershipApply, OwnershipRegistry};
pub use ownership_source::{OwnershipEventSource, OwnershipMessage};
pub use platform_portfolio_ownership::AccountKey;
pub use portfolio::{
    AccountPortfolio, AccountPortfolios, PORTFOLIO_UPDATE_SCHEMA_VERSION,
    PORTFOLIO_UPDATE_STREAM_PREFIX, PORTFOLIO_UPDATE_SUBJECT, PendingOrder, PortfolioUpdate,
};
pub use publishing_sink::PublishingPortfolioUpdateSink;
pub use runner::PortfolioRunner;
pub use service::PortfolioService;
pub use state::{PortfolioState, PortfolioUpdateMetadata, PortfolioUpdateOutcome};
pub use update_consumer::PortfolioUpdateConsumer;
pub use update_source::{PortfolioUpdateMessage, PortfolioUpdateSource};

#[cfg(test)]
mod tests;
