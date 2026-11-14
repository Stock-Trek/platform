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
    let results = SimulationPipeline::run(8, 3, Fixture::value).unwrap();
    assert_eq!(results.len(), 8);
    assert_eq!(
        results.into_iter().collect::<Result<Vec<_>, _>>().unwrap(),
        (0..8).map(Value::from).collect::<Vec<_>>()
    );
}

#[test]
fn simulations_respect_the_configured_concurrency_limit() {
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let barrier = Barrier::new(3);
    let results = SimulationPipeline::run(12, 3, |index| {
        let running = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(running, Ordering::SeqCst);
        barrier.wait();
        active.fetch_sub(1, Ordering::SeqCst);
        Fixture::value(index)
    })
    .unwrap();
    assert_eq!(results.len(), 12);
    assert!(results.iter().all(Result::is_ok));
    assert!(peak.load(Ordering::SeqCst) <= 3);
}

#[test]
fn producer_failures_are_kept_in_their_simulation_slots() {
    let results = SimulationPipeline::run(3, 2, |index| {
        if index == 1 {
            Err(BacktestError::Panic)
        } else {
            Fixture::value(index)
        }
    })
    .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
}

#[test]
fn a_panicking_producer_is_reported_without_losing_other_results() {
    let results = SimulationPipeline::run(4, 2, |index| {
        if index == 1 {
            panic!("simulation failed");
        }
        Fixture::value(index)
    })
    .unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(
        results[1].as_ref().unwrap_err().to_string(),
        BacktestError::Panic.to_string()
    );
}

#[test]
fn zero_simulations_are_rejected() {
    let error = SimulationPipeline::run(0, 2, Fixture::value).unwrap_err();
    assert!(error.to_string().contains("at least one"));
}
