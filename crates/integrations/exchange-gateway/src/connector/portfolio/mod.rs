mod account_watcher;
mod config;
mod connector_health;
mod connector_health_contract;
mod connector_health_sink;
mod connector_portfolio_update;
mod connector_state;
mod exchange_portfolio;
mod exchange_portfolio_connection;
mod exchange_portfolio_connector;
mod exchange_portfolio_feed;
mod health_reporter;
mod noop_connector_health_sink;
mod noop_portfolio_feed;
mod portfolio_accounts;
mod portfolio_connection;
mod portfolio_connector;
mod portfolio_credential_source;
mod portfolio_exchange;
mod portfolio_feed;
mod portfolio_session;
mod portfolio_stream;
mod portfolio_update_sink;
mod session_state;
mod watcher;

#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod connector_tests;
#[cfg(test)]
mod feed_tests;
#[cfg(test)]
mod health_tests;
#[cfg(test)]
mod portfolio_test_fakes_tests;
#[cfg(test)]
mod session_tests;

pub use config::{PortfolioConnectorConfig, PortfolioExchangeConfig};
pub use connector_health::ConnectorHealth;
pub use connector_health_contract::{CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT};
pub use connector_health_sink::ConnectorHealthSink;
pub use connector_portfolio_update::ConnectorPortfolioUpdate;
pub use connector_state::ConnectorState;
pub use exchange_portfolio::ExchangePortfolio;
pub use exchange_portfolio_connector::ExchangePortfolioConnector;
pub use exchange_portfolio_feed::ExchangePortfolioFeed;
pub use noop_connector_health_sink::NoopConnectorHealthSink;
pub use noop_portfolio_feed::NoopPortfolioFeed;
pub use portfolio_accounts::PortfolioAccounts;
pub use portfolio_connection::PortfolioConnection;
pub use portfolio_connector::PortfolioConnector;
pub use portfolio_credential_source::PortfolioCredentialSource;
pub use portfolio_exchange::PortfolioExchange;
pub use portfolio_feed::PortfolioFeed;
pub use portfolio_session::PortfolioSession;
pub use portfolio_stream::{PortfolioEvent, PortfolioStream};
pub use portfolio_update_sink::{PortfolioUpdateOutcome, PortfolioUpdateSink};
