use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::cwasm::backtest::BacktestError;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub struct WorkerRuntime {
    runtime: tokio::runtime::Runtime,
    id: usize,
}

impl WorkerRuntime {
    pub fn new() -> Result<Self, BacktestError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(BacktestError::RuntimeStart)?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Ok(Self { runtime, id })
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}
