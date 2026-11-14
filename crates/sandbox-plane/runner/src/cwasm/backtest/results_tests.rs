use serde_json::{Value, json};

use crate::cwasm::backtest::results::BacktestResults;

struct Fixture;

impl Fixture {
    fn simulation(index: u32) -> Value {
        json!({
            "simulationId": format!("back-test-{index}"),
            "simulationSeed": index,
        })
    }
}

#[test]
fn collates_every_simulation_into_one_value() {
    let simulations = (0..3).map(Fixture::simulation).collect::<Vec<_>>();
    let collated = BacktestResults::collate(simulations.clone());
    assert_eq!(collated, Value::Array(simulations));
}

#[test]
fn preserves_simulation_order() {
    let collated = BacktestResults::collate((0..4).rev().map(Fixture::simulation).collect());
    let simulations = collated.as_array().unwrap();
    assert_eq!(simulations.len(), 4);
    for (position, simulation) in simulations.iter().enumerate() {
        assert_eq!(
            simulation.get("simulationId").and_then(Value::as_str),
            Some(format!("back-test-{}", 3 - position).as_str())
        );
    }
}

#[test]
fn collates_an_empty_result_set() {
    assert_eq!(BacktestResults::collate(Vec::new()), json!([]));
}
