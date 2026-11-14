use crate::{
    MemoryTokenUsageQueue, TokenStore, TokenUsage, TokenUsageQueue, TokenUsageReservation,
    TokenUsageWorker,
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Test queue that applies usage immediately, preserving synchronous test
/// expectations while production code always goes through the durable queue.
pub struct ImmediateTokenUsageQueue {
    store: Arc<dyn TokenStore>,
    inner: Arc<MemoryTokenUsageQueue>,
}

impl ImmediateTokenUsageQueue {
    pub fn new(store: Arc<dyn TokenStore>) -> Self {
        Self {
            store,
            inner: Arc::new(MemoryTokenUsageQueue::new(usize::MAX)),
        }
    }

    async fn drain(&self) -> BuildResult<()> {
        TokenUsageWorker::new(self.store.clone(), self.inner.clone(), 100)
            .drain()
            .await?;
        Ok(())
    }
}

#[async_trait]
impl TokenUsageQueue for ImmediateTokenUsageQueue {
    async fn enqueue(&self, usage: TokenUsage) -> BuildResult<()> {
        self.inner.enqueue(usage).await?;
        self.drain().await
    }

    async fn reserve(&self, limit: usize) -> BuildResult<Vec<TokenUsageReservation>> {
        self.inner.reserve(limit).await
    }

    async fn acknowledge(&self, reservation: &TokenUsageReservation) -> BuildResult<()> {
        self.inner.acknowledge(reservation).await
    }

    async fn release(&self, reservation: &TokenUsageReservation) -> BuildResult<()> {
        self.inner.release(reservation).await
    }
}

pub struct ToggleTokenUsageQueue {
    inner: MemoryTokenUsageQueue,
    failing: AtomicBool,
}

impl ToggleTokenUsageQueue {
    pub fn new() -> Self {
        Self {
            inner: MemoryTokenUsageQueue::new(1024),
            failing: AtomicBool::new(false),
        }
    }

    pub fn fail(&self) {
        self.failing.store(true, Ordering::SeqCst);
    }

    pub fn succeed(&self) {
        self.failing.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl TokenUsageQueue for ToggleTokenUsageQueue {
    async fn enqueue(&self, usage: TokenUsage) -> BuildResult<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(BuildError::auth_store(
                "usage",
                "test-queue",
                true,
                "queue unavailable".to_string(),
            ));
        }
        self.inner.enqueue(usage).await
    }

    async fn reserve(&self, limit: usize) -> BuildResult<Vec<TokenUsageReservation>> {
        self.inner.reserve(limit).await
    }

    async fn acknowledge(&self, reservation: &TokenUsageReservation) -> BuildResult<()> {
        self.inner.acknowledge(reservation).await
    }

    async fn release(&self, reservation: &TokenUsageReservation) -> BuildResult<()> {
        self.inner.release(reservation).await
    }
}
