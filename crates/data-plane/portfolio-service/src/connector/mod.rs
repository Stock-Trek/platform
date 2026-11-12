mod binance;
mod connection;
mod credentials;
mod feed;
mod health;
mod health_reporter;
mod port;
mod session_state;
mod transport;
mod update;
mod watcher;

pub use binance::BinanceExchangeTransport;
pub use connection::ExchangePortfolioConnector;
pub use credentials::{PortfolioCredentialSource, StaticPortfolioCredentials};
pub use feed::{ExchangePortfolioFeed, NoopPortfolioFeed, PortfolioFeed};
pub use health::{
    CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorHealthSink,
    ConnectorState, NoopConnectorHealthSink,
};
pub use port::{
    PortfolioConnection, PortfolioConnector, PortfolioUpdateApply, PortfolioUpdateSink,
};
pub use transport::{
    PortfolioEvent, PortfolioExchangeTransport, PortfolioPosition, PortfolioSession,
    PortfolioSnapshot,
};
pub use update::ConnectorPortfolioUpdate;
