pub mod auth_binder;
pub mod config;
pub mod credential_client;
pub mod credential_provider;
pub mod credentials;
pub mod event_parser;
pub mod listen_key;
pub mod session_lifecycle;
pub mod user_data_event;

pub use auth_binder::BinanceAuthBinder;
pub use credential_client::BinanceListenKeyClient;
pub use credential_provider::BinanceCredentialProvider;
pub use session_lifecycle::{BinanceSessionLifecycle, BinanceSessionLifecyclePolicy};

#[cfg(test)]
mod auth_tests;
#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod event_parser_tests;
#[cfg(test)]
mod listen_key_tests;
#[cfg(test)]
mod user_data_tests;
