use parking_lot::Mutex;
use serde_json::Value;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc,
    },
};

use crate::cwasm::backtest::{BacktestError, worker_runtime::WorkerRuntime};

pub const PUBLISHER_QUEUE_DEPTH: usize = 2;

pub struct PublisherPipeline;

impl PublisherPipeline {
    pub fn run<P, Q>(
        simulation_count: u32,
        worker_count: u32,
        publisher_count: u32,
        produce: P,
        publish: Q,
    ) -> Result<Vec<Result<(), BacktestError>>, BacktestError>
    where
        P: Fn(u32) -> Result<Value, BacktestError> + Sync,
        Q: Fn(&WorkerRuntime, u32, &Value) -> Result<(), BacktestError> + Sync,
    {
        let worker_count = worker_count.max(1);
        let publisher_count = publisher_count.max(1);
        let runtimes = (0..publisher_count)
            .map(|_| WorkerRuntime::new())
            .collect::<Result<Vec<_>, _>>()?;
        let outcomes = Mutex::new(Vec::new());
        let next = AtomicU32::new(0);
        let (sender, receiver) =
            mpsc::sync_channel(publisher_count as usize * PUBLISHER_QUEUE_DEPTH);
        let receiver = Mutex::new(receiver);
        std::thread::scope(|scope| {
            for runtime in runtimes {
                let receiver = &receiver;
                let outcomes = &outcomes;
                let publish = &publish;
                scope.spawn(move || {
                    loop {
                        let job = {
                            let receiver = receiver.lock();
                            receiver.recv()
                        };
                        let Ok((index, result)) = job else {
                            break;
                        };
                        let outcome =
                            catch_unwind(AssertUnwindSafe(|| publish(&runtime, index, &result)))
                                .unwrap_or(Err(BacktestError::Panic));
                        outcomes.lock().push((index, outcome));
                    }
                });
            }
            for _ in 0..worker_count {
                let sender = sender.clone();
                let next = &next;
                let outcomes = &outcomes;
                let produce = &produce;
                scope.spawn(move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= simulation_count {
                            break;
                        }
                        let result = catch_unwind(AssertUnwindSafe(|| produce(index)))
                            .unwrap_or(Err(BacktestError::Panic));
                        match result {
                            Ok(result) => {
                                if sender.send((index, result)).is_err() {
                                    outcomes.lock().push((
                                        index,
                                        Err(BacktestError::Publish(
                                            "publisher pipeline closed".to_string(),
                                        )),
                                    ));
                                    break;
                                }
                            }
                            Err(error) => outcomes.lock().push((index, Err(error))),
                        }
                    }
                });
            }
            drop(sender);
        });
        let mut outcomes = outcomes.into_inner();
        outcomes.sort_by_key(|(index, _)| *index);
        Ok(outcomes
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>())
    }
}
