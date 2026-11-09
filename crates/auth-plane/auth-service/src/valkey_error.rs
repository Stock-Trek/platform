use crate::ReplayStoreError;
use platform_errors::{BpError, BpResult};
use redis::{ErrorKind, RedisError};

const TARGET: &str = "valkey-replay-store";

pub(crate) struct ValkeyError;

impl ValkeyError {
    pub(crate) fn store(operation: &'static str, error: RedisError) -> ReplayStoreError {
        ReplayStoreError::unavailable(operation, Self::retryable(&error), error)
    }

    pub(crate) fn connect(error: ReplayStoreError) -> BpError {
        BpError::auth_store("connect", TARGET, error.is_retryable(), error)
    }

    pub(crate) fn result<T>(result: Result<T, ReplayStoreError>) -> BpResult<T> {
        result.map_err(Self::connect)
    }

    fn retryable(error: &RedisError) -> bool {
        error.is_io_error()
            || error.is_timeout()
            || error.is_connection_dropped()
            || error.is_cluster_error()
            || matches!(
                error.kind(),
                ErrorKind::BusyLoadingError
                    | ErrorKind::TryAgain
                    | ErrorKind::ClusterDown
                    | ErrorKind::MasterDown
                    | ErrorKind::ReadOnly
            )
    }
}
