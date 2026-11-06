use std::time::Duration;

pub const DEFAULT_EXECUTION_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_GUEST_MEMORY_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_MAX_TABLE_ELEMENTS: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    pub timeout: Duration,
    pub max_memory_bytes: usize,
    pub max_table_elements: usize,
}

impl ExecutionLimits {
    pub const fn new(timeout: Duration, max_memory_bytes: usize) -> Self {
        Self::with_table_elements(timeout, max_memory_bytes, DEFAULT_MAX_TABLE_ELEMENTS)
    }

    pub const fn with_table_elements(
        timeout: Duration,
        max_memory_bytes: usize,
        max_table_elements: usize,
    ) -> Self {
        Self {
            timeout,
            max_memory_bytes,
            max_table_elements,
        }
    }
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self::new(DEFAULT_EXECUTION_TIMEOUT, DEFAULT_MAX_GUEST_MEMORY_BYTES)
    }
}
