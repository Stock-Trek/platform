mod clients;
mod connector;
mod panic_guard;
mod rate_limit;
mod resync_connector;
mod retry_after_header;
mod server_clock;
mod submission;
mod websocket_listener;

#[cfg(test)]
mod resync_connector_tests;
#[cfg(test)]
mod retry_after_header_tests;
#[cfg(test)]
mod server_clock_tests;

pub use async_trait::async_trait;
pub use clients::{iris::IrisWebsocketClient, reqwest::ReqwestHttpClient};
pub use connector::{
    CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT, Connector, ConnectorHealth,
    ConnectorHealthSink, ConnectorPortfolioUpdate, ConnectorState, ExchangePortfolio,
    ExchangePortfolioConnector, ExchangePortfolioFeed, NoopConnectorHealthSink, NoopPortfolioFeed,
    PortfolioAccounts, PortfolioConnection, PortfolioConnector, PortfolioConnectorConfig,
    PortfolioCredentialSource, PortfolioEvent, PortfolioExchange, PortfolioExchangeConfig,
    PortfolioFeed, PortfolioSession, PortfolioStream, PortfolioUpdateOutcome, PortfolioUpdateSink,
};
pub use iris;
pub use resync_connector::AutoResyncConnector;
pub use server_clock::Clock;
pub use submission::{Submission, SubmissionOutcome};
pub use websocket_listener::{WaiterForResponse, WebsocketListener};
