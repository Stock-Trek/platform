pub mod binance;
mod connector;
mod feed;
mod health;
mod update;

pub use connector::{
    PortfolioConnection, PortfolioConnector, PortfolioUpdateApply, PortfolioUpdateSink,
};
pub use feed::{ExchangePortfolioFeed, NoopPortfolioFeed, PortfolioFeed};
pub use health::{
    CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorHealthSink,
    ConnectorState, NoopConnectorHealthSink,
};
pub use update::PortfolioUpdate;

#[cfg(test)]
mod feed_tests;
