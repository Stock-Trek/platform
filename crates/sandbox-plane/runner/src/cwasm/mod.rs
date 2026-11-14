//! Download a compiled runner artifact from a pre-signed URL.
//!
//! The control plane signs an S3 `GetObject` request and passes the URL in the
//! run request as `cwasmGetUrl`. The artifact is streamed to a private
//! temporary file so it does not need to fit in memory.
//!
//! Guests export `alloc`, `memory` and `entry`; the real runner also exports
//! `initialize`, which the invoker calls before `entry` so wasm constructors
//! (including the algorithm registry) run before the command is handled.

pub mod backtest;
mod cwasm_artifact;
mod guest_actions;
mod guest_state;
mod invoker;
mod jwt_cache;
mod jwt_fetcher;
mod limits;
mod order_request_dispatcher;
mod runtime;
mod watchdog;
mod workspace;

#[cfg(test)]
mod invoker_tests;
#[cfg(test)]
mod jwt_cache_tests;
#[cfg(test)]
mod order_request_dispatcher_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod tests;

pub use cwasm_artifact::{
    CWASM_FILE_NAME, Cwasm, CwasmIntegrity, CwasmLimits, DEFAULT_CWASM_DOWNLOAD_TIMEOUT,
    DEFAULT_MAX_CWASM_BYTES, WASMTIME_VERSION,
};
pub use guest_actions::GuestActions;
pub use guest_state::Emit;
pub use invoker::InvokerConfig;
pub use jwt_cache::JwtCache;
pub use limits::{
    DEFAULT_EXECUTION_TIMEOUT, DEFAULT_MAX_GUEST_MEMORY_BYTES, DEFAULT_MAX_TABLE_ELEMENTS,
    ExecutionLimits,
};
pub use order_request_dispatcher::OrderRequestDispatcher;
pub use runtime::WasmRunner;
pub use workspace::{GuestWorkspace, PORTFOLIO_FILE_NAME, STRATEGY_CONTEXT_FILE_NAME};
