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

pub const SIMULATION_QUEUE_DEPTH: usize = 2;

#[derive(Debug)]
pub struct SimulationOutcome {
    pub result: Result<Value, BacktestError>,
    pub stream_error: Option<BacktestError>,
}

pub struct SimulationPipeline;

impl SimulationPipeline {
    pub fn run<P, S>(
        simulation_count: u32,
        worker_count: u32,
        publisher_count: u32,
        produce: P,
        stream: S,
    ) -> Result<Vec<SimulationOutcome>, BacktestError>
    where
        P: Fn(u32) -> Result<Value, BacktestError> + Sync,
        S: Fn(&WorkerRuntime, &Value) -> Result<(), BacktestError> + Sync,
    {
        if simulation_count == 0 {
            return Err(BacktestError::invalid_argument(
                "simulation count must be at least one",
            ));
        }
        let worker_count = worker_count.max(1).min(simulation_count);
        let publisher_count = publisher_count.max(1).min(simulation_count);
        let runtimes = (0..publisher_count)
            .map(|_| WorkerRuntime::new())
            .collect::<Result<Vec<_>, _>>()?;
        let next = AtomicU32::new(0);
        let outcomes = Mutex::new(Vec::new());
        let (sender, receiver) =
            mpsc::sync_channel(publisher_count as usize * SIMULATION_QUEUE_DEPTH);
        let receiver = Mutex::new(receiver);
        std::thread::scope(|scope| {
            for runtime in runtimes {
                let receiver = &receiver;
                let outcomes = &outcomes;
                let stream = &stream;
                scope.spawn(move || {
                    loop {
                        let job = {
                            let receiver = receiver.lock();
                            receiver.recv()
                        };
                        let Ok((index, result)) = job else {
                            break;
                        };
                        let stream_error =
                            catch_unwind(AssertUnwindSafe(|| stream(&runtime, &result)))
                                .unwrap_or(Err(BacktestError::Panic))
                                .err();
                        outcomes.lock().push((
                            index,
                            SimulationOutcome {
                                result: Ok(result),
                                stream_error,
                            },
                        ));
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
                                        SimulationOutcome {
                                            result: Err(BacktestError::Publish(
                                                "publisher pipeline closed".to_string(),
                                            )),
                                            stream_error: None,
                                        },
                                    ));
                                    break;
                                }
                            }
                            Err(error) => outcomes.lock().push((
                                index,
                                SimulationOutcome {
                                    result: Err(error),
                                    stream_error: None,
                                },
                            )),
                        }
                    }
                });
            }
            drop(sender);
        });
        let mut outcomes = outcomes.into_inner();
        outcomes.sort_by_key(|(index, _)| *index);
        Ok(outcomes.into_iter().map(|(_, outcome)| outcome).collect())
    }
}
