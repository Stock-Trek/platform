mod auth_service;
mod config;
mod feed;
mod nats_ownership_source;
mod order_delivery;
mod order_request;
mod order_request_connection;
mod order_request_error;
mod order_request_server;
mod ownership_consumer;
mod ownership_registry;
mod ownership_source;
mod portfolio;
mod runner;
mod service;
mod state;

pub use auth_service::PortfolioAuthService;
pub use config::{AuthConfig, OrderRequestConfig, OwnershipConfig, PortfolioConfig};
pub use feed::{
    GroupingPortfolioFeed, NoopPortfolioFeed, PortfolioConnection, PortfolioConnector,
    PortfolioFeed,
};
pub use nats_ownership_source::NatsOwnershipSource;
pub use order_delivery::OrderRequestDelivery;
pub use order_request::{OrderRequestService, SignedOrderSubmission};
pub use order_request_error::OrderRequestError;
pub use order_request_server::OrderRequestServer;
pub use ownership_consumer::OwnershipConsumer;
pub use ownership_registry::{OwnershipApply, OwnershipRegistry};
pub use ownership_source::{OwnershipEventSource, OwnershipMessage};
pub use platform_portfolio_ownership::AccountKey;
pub use portfolio::{AccountPortfolio, AccountPortfolios, PendingOrder, PortfolioUpdate};
pub use runner::PortfolioRunner;
pub use service::PortfolioService;
pub use state::PortfolioState;

#[cfg(test)]
mod tests;
