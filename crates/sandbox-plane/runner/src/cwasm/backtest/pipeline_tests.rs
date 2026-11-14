use parking_lot::Mutex;
use serde_json::{Value, json};
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

use crate::cwasm::backtest::{BacktestError, pipeline::SimulationPipeline};

struct Fixture;

impl Fixture {
    fn value(index: u32) -> Result<Value, BacktestError> {
        Ok(json!(index))
    }
}

#[test]
fn runs_every_simulation_and_returns_results_in_order() {
    let outcomes = SimulationPipeline::run(8, 3, 3, Fixture::value, |_, _| Ok(())).unwrap();
    assert_eq!(outcomes.len(), 8);
    assert_eq!(
        outcomes
            .into_iter()
            .map(|outcome| outcome.result.unwrap())
            .collect::<Vec<_>>(),
        (0..8).map(Value::from).collect::<Vec<_>>()
    );
}

#[test]
fn streams_every_produced_result() {
    let streamed = Mutex::new(Vec::new());
    let outcomes = SimulationPipeline::run(8, 3, 3, Fixture::value, |_, result| {
        streamed.lock().push(result.as_u64().unwrap());
        Ok(())
    })
    .unwrap();
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.stream_error.is_none())
    );
    let mut streamed = streamed.into_inner();
    streamed.sort_unstable();
    assert_eq!(streamed, (0..8).collect::<Vec<_>>());
}

#[test]
fn simulations_respect_the_configured_concurrency_limit() {
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let barrier = Barrier::new(3);
    let outcomes = SimulationPipeline::run(
        12,
        3,
        3,
        |index| {
            let running = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(running, Ordering::SeqCst);
            barrier.wait();
            active.fetch_sub(1, Ordering::SeqCst);
            Fixture::value(index)
        },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(outcomes.len(), 12);
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    assert!(peak.load(Ordering::SeqCst) <= 3);
}

#[test]
fn producer_failures_are_kept_in_their_simulation_slots() {
    let outcomes = SimulationPipeline::run(
        3,
        2,
        2,
        |index| {
            if index == 1 {
                Err(BacktestError::Panic)
            } else {
                Fixture::value(index)
            }
        },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].result.is_ok());
    assert!(outcomes[1].result.is_err());
    assert!(outcomes[2].result.is_ok());
}

#[test]
fn a_panicking_producer_is_reported_without_losing_other_results() {
    let outcomes = SimulationPipeline::run(
        4,
        2,
        2,
        |index| {
            if index == 1 {
                panic!("simulation failed");
            }
            Fixture::value(index)
        },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(outcomes.len(), 4);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.result.is_err())
            .count(),
        1
    );
    assert_eq!(
        outcomes[1].result.as_ref().unwrap_err().to_string(),
        BacktestError::Panic.to_string()
    );
}

#[test]
fn stream_failures_do_not_discard_produced_results() {
    let outcomes = SimulationPipeline::run(3, 2, 2, Fixture::value, |_, result| {
        if result.as_u64() == Some(1) {
            Err(BacktestError::Publish("stream failed".to_string()))
        } else {
            Ok(())
        }
    })
    .unwrap();
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.stream_error.is_some())
            .count(),
        1
    );
    assert_eq!(
        outcomes[1].stream_error.as_ref().unwrap().to_string(),
        "failed to publish simulation result: stream failed"
    );
}

#[test]
fn zero_simulations_are_rejected() {
    let error = SimulationPipeline::run(0, 2, 2, Fixture::value, |_, _| Ok(())).unwrap_err();
    assert!(error.to_string().contains("at least one"));
}
