use parking_lot::Mutex;
use serde_json::{Value, json};
use std::collections::BTreeMap;

use crate::cwasm::backtest::{WorkerRuntime, pipeline::PublisherPipeline};

#[test]
fn publishes_every_result_in_simulation_order() {
    let published = Mutex::new(Vec::new());
    let results = PublisherPipeline::run(
        8,
        3,
        3,
        |index| Ok(json!(index)),
        |_, index, _| {
            published.lock().push(index);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(results.len(), 8);
    assert!(results.iter().all(Result::is_ok));
    let published = published.into_inner();
    assert_eq!(published, (0..8).collect::<Vec<_>>());
}

#[test]
fn reuses_worker_runtimes_across_multiple_simulations() {
    let runtime_ids = Mutex::new(Vec::new());
    let results = PublisherPipeline::run(
        12,
        3,
        3,
        |index| Ok(Value::from(index)),
        |runtime, _, _| {
            runtime_ids.lock().push(runtime.id());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(results.len(), 12);
    let runtime_ids = runtime_ids.into_inner();
    assert_eq!(runtime_ids.len(), 12);
    let mut uses = BTreeMap::new();
    for id in runtime_ids {
        *uses.entry(id).or_insert(0) += 1;
    }
    assert!(uses.len() <= 3);
    assert!(uses.values().any(|uses| *uses > 1));
}

#[test]
fn a_panicking_publisher_is_reported_without_losing_other_results() {
    let results = PublisherPipeline::run(
        4,
        2,
        2,
        |index| Ok(Value::from(index)),
        |_, index, _| {
            if index == 1 {
                panic!("publish failed");
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
}

#[test]
fn producer_failures_are_kept_in_their_simulation_slots() {
    let results = PublisherPipeline::run(
        3,
        2,
        2,
        |index| {
            if index == 1 {
                Err(crate::cwasm::backtest::BacktestError::Panic)
            } else {
                Ok(Value::from(index))
            }
        },
        |_, _, _| Ok(()),
    )
    .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
}

#[test]
fn worker_runtime_identity_is_stable_for_its_lifetime() {
    let runtime = WorkerRuntime::new().unwrap();
    assert_eq!(runtime.id(), runtime.id());
}
