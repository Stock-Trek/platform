use serde_json::Value;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc,
    },
};

use crate::cwasm::backtest::BacktestError;

pub const SIMULATION_QUEUE_DEPTH: usize = 2;

pub struct SimulationPipeline;

impl SimulationPipeline {
    pub fn run<P>(
        simulation_count: u32,
        worker_count: u32,
        produce: P,
    ) -> Result<Vec<Result<Value, BacktestError>>, BacktestError>
    where
        P: Fn(u32) -> Result<Value, BacktestError> + Sync,
    {
        if simulation_count == 0 {
            return Err(BacktestError::invalid_argument(
                "simulation count must be at least one",
            ));
        }
        let worker_count = worker_count.max(1).min(simulation_count);
        let next = AtomicU32::new(0);
        let (sender, receiver) = mpsc::sync_channel(worker_count as usize * SIMULATION_QUEUE_DEPTH);
        let mut outcomes = Vec::new();
        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                let sender = sender.clone();
                let next = &next;
                let produce = &produce;
                scope.spawn(move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= simulation_count {
                            break;
                        }
                        let result = catch_unwind(AssertUnwindSafe(|| produce(index)))
                            .unwrap_or(Err(BacktestError::Panic));
                        if sender.send((index, result)).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(sender);
            for outcome in receiver {
                outcomes.push(outcome);
            }
        });
        outcomes.sort_by_key(|(index, _)| *index);
        Ok(outcomes
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>())
    }
}
