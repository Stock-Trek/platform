mod balances;
mod config;
mod connection;
mod credentials;
mod event_parser;
mod gateway_transport;
mod health_reporter;
mod session;
mod session_state;
mod transport;

pub use config::BinancePortfolioConfig;
pub use connection::BinancePortfolioConnector;
pub use credentials::{BinanceCredentialSource, StaticBinanceCredentials};
pub use gateway_transport::GatewayBinanceTransport;
pub use transport::{
    AccountPositionEvent, AccountSnapshot, BinanceAccountEvent, BinanceEventStream,
    BinanceTransport,
};

#[cfg(test)]
mod event_parser_tests;
#[cfg(test)]
mod tests;
