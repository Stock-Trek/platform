mod api;
mod config;
mod dynamodb_ownership_store;
mod mapping;
#[cfg(test)]
mod memory_ownership_store;
mod outbox_relay;
mod ownership_command;
mod ownership_item;
mod ownership_record;
mod ownership_store;
mod ownership_transition;
mod registry_service;
mod runner;

pub use config::{
    AdminConfig, PortfolioRegistryConfig, ReadConfig, RegistryNatsConfig, RegistryStoreConfig,
};
pub use dynamodb_ownership_store::{DynamoDbOwnershipStore, OWNER_INDEX};
pub use mapping::AccountMapping;
#[cfg(test)]
pub(crate) use memory_ownership_store::MemoryOwnershipStore;
pub(crate) use outbox_relay::OwnershipOutboxRelay;
pub use registry_service::RegistryService;
pub use runner::RegistryRunner;

pub(crate) use api::{AdminApi, ApiServer, ReadApi};
pub(crate) use ownership_command::OwnershipCommand;
pub(crate) use ownership_item::{
    ACCOUNT_KEY, ACTIVE, OWNER, OwnershipItem, PENDING_EVENTS, VERSION,
};
pub(crate) use ownership_record::OwnershipRecord;
pub(crate) use ownership_store::{OwnershipCommit, OwnershipStore};
pub(crate) use ownership_transition::OwnershipTransition;

#[cfg(test)]
mod tests;
