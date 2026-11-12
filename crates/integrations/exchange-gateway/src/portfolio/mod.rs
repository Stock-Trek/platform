pub mod binance;
mod connection;
mod connector;
mod connector_config;
mod credentials;
mod feed;
mod health;
mod health_reporter;
mod session_state;
mod transport;
mod update;
mod watcher;

pub use connection::ExchangePortfolioConnector;
pub use connector::{
    PortfolioConnection, PortfolioConnector, PortfolioUpdateApply, PortfolioUpdateSink,
};
pub use connector_config::PortfolioConnectorConfig;
pub use credentials::{PortfolioCredentialSource, StaticPortfolioCredentials};
pub use feed::{ExchangePortfolioFeed, NoopPortfolioFeed, PortfolioFeed};
pub use health::{
    CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorHealthSink,
    ConnectorState, NoopConnectorHealthSink,
};
pub use transport::{
    PortfolioEvent, PortfolioExchangeTransport, PortfolioPosition, PortfolioSession,
    PortfolioSnapshot,
};
pub use update::PortfolioUpdate;

#[cfg(test)]
mod connection_tests;
#[cfg(test)]
mod feed_tests;
