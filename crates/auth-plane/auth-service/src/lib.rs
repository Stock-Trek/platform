mod api_token;
mod audit;
mod auth_service;
mod cached_token_store;
mod circuit_breaker;
mod entry;
mod flight_gates;
mod jwks_cache;
mod jwt_server;
mod jwt_server_config;
mod jwt_server_connection;
mod jwt_server_request;
mod jwt_signing_key;
mod jwt_verifier;
mod order_jwt;
mod order_request_context;
mod order_signature_validation;
mod order_signing;
mod order_verification;
mod owner_index;
mod owner_index_reconciliation;
mod replay_store;
mod secret_input;
mod signed_order_request;
mod token_administration;
mod token_audit_relay;
mod token_cache;
mod token_cache_config;
mod token_cache_loader;
mod token_lifecycle;
mod token_metadata;
mod token_metadata_value;
mod token_owner_index;
mod token_rotation;
mod token_rotation_plan;
mod token_rotation_transaction;
mod token_scope;
mod token_service;
mod token_store;
mod token_usage;
mod token_usage_queue_config;
mod token_usage_worker;
mod token_value;
mod valkey_config;
mod valkey_replay_store;
mod valkey_token_usage_queue;
mod vault_client;
mod vault_commit;
mod vault_commit_events;
mod vault_config;
mod vault_error;
mod vault_owner_index;
mod vault_read;
mod vault_token_store;
mod vault_write;

#[cfg(any(test, feature = "test-utils"))]
mod memory_replay_store_tests;
#[cfg(any(test, feature = "test-utils"))]
mod memory_token_store_tests;
#[cfg(any(test, feature = "test-utils"))]
mod memory_token_usage_queue_tests;
#[cfg(test)]
mod tests;

pub use api_token::ApiToken;
pub use audit::{
    MemoryTokenAuditLog, SqsTokenAuditLog, TokenAuditAction, TokenAuditEvent, TokenAuditLog,
};
pub use auth_service::{AuthService, AuthServiceConfig};
pub use cached_token_store::CachedTokenStore;
pub use circuit_breaker::BreakerState;
pub use entry::{AuthServiceRunner, Entry};
pub use jwks_cache::JwksCache;
pub use jwt_server::JwtServer;
pub use jwt_server_config::JwtServerConfig;
pub use jwt_signing_key::{DEFAULT_JWT_KEY_ID, JWT_SIGNING_ALGORITHM, JwtSigningKey};
pub use jwt_verifier::JwtVerifier;
#[cfg(any(test, feature = "test-utils"))]
pub use memory_replay_store_tests::{DEFAULT_REPLAY_CAPACITY, MemoryReplayStore};
#[cfg(any(test, feature = "test-utils"))]
pub use memory_token_store_tests::MemoryTokenStore;
#[cfg(any(test, feature = "test-utils"))]
pub use memory_token_usage_queue_tests::MemoryTokenUsageQueue;
pub use order_jwt::{IssuedJwt, OrderJwtClaims};
pub use order_request_context::OrderRequestContext;
pub use order_signing::OrderRequestSigner;
pub use order_verification::{InvalidOrderRequest, OrderRequestVerification};
pub use owner_index::{OWNER_INDEX_PREFIX, OwnerIndex, OwnerIndexEntry};
pub use replay_store::{
    ConsumeResult, ReplayKey, ReplayReservation, ReplayStore, ReplayStoreError,
};
pub use secret_input::SecretInput;
pub use signed_order_request::SignedOrderRequest;
pub use token_administration::{TokenAdministrator, TokenReader};
pub use token_audit_relay::TokenAuditRelay;
pub use token_cache_config::TokenCacheConfig;
pub use token_metadata::TokenMetadata;
pub use token_rotation_transaction::TokenRotationTransaction;
pub use token_scope::{
    RevocationReason, RevokedBy, TokenExpiry, TokenOptions, TokenScope, TokenView,
};
pub use token_store::{TokenMutation, TokenStore};
pub use token_usage::{TokenUsage, TokenUsageQueue, TokenUsageReservation};
pub use token_usage_queue_config::{
    DEFAULT_TOKEN_USAGE_QUEUE_BATCH_SIZE, DEFAULT_TOKEN_USAGE_QUEUE_CAPACITY,
    DEFAULT_TOKEN_USAGE_QUEUE_PREFIX, TokenUsageQueueConfig,
};
pub use token_usage_worker::TokenUsageWorker;
pub use valkey_config::{DEFAULT_VALKEY_REPLAY_PREFIX, ValkeyReplayStoreConfig};
pub use valkey_replay_store::ValkeyReplayStore;
pub use valkey_token_usage_queue::ValkeyTokenUsageQueue;
pub use vault_config::VaultTokenStoreConfig;
pub use vault_token_store::VaultTokenStore;
