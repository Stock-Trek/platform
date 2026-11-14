use serde_json::Value;
use std::{collections::BTreeMap, path::Path};
use stock_trek::types::{AccountId, AssetId, CexId};

use crate::Command;
use crate::cwasm::WasmRunner;

use crate::cwasm::backtest::{
    BacktestError, pipeline::SimulationPipeline, publish::ResultPublisher,
    results::BacktestResults, simulation::Simulation, spec::Spec, worker_limits::WorkerLimits,
    worker_runtime::WorkerRuntime,
};

pub type Balances = BTreeMap<CexId, BTreeMap<AccountId, BTreeMap<AssetId, f64>>>;
pub type PriceMap = BTreeMap<(CexId, AssetId, AssetId), f64>;

pub struct Backtest;

impl Backtest {
    pub fn run(
        runner: &WasmRunner,
        cwasm_path: &Path,
        command: &Command,
    ) -> Result<(), BacktestError> {
        let spec = Spec::new(command)?;
        let limits = WorkerLimits::global();
        let worker_count =
            limits.worker_count(spec.simulation_count, spec.max_concurrent_simulations);
        let outcomes = SimulationPipeline::run(
            spec.simulation_count,
            worker_count,
            worker_count,
            |index| Simulation::run(runner, cwasm_path, &spec, index),
            |runtime, result| runtime.block_on(ResultPublisher::stream(&spec, result)),
        )?;
        let mut simulations = Vec::new();
        let mut first_error = None;
        for outcome in outcomes {
            match outcome.result {
                Ok(value) => simulations.push(value),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
            if let Some(error) = outcome.stream_error {
                first_error.get_or_insert(error);
            }
        }
        let result = BacktestResults::collate(simulations);
        let post = WorkerRuntime::new()?.block_on(ResultPublisher::post(&spec, &result));
        match (first_error, post) {
            (Some(error), _) => Err(error),
            (None, Err(error)) => Err(error),
            (None, Ok(())) => Ok(()),
        }
    }

    pub fn run_simulations<F>(
        simulation_count: u32,
        max_concurrent: u32,
        simulate: F,
    ) -> Result<Vec<Result<(), BacktestError>>, BacktestError>
    where
        F: Fn(u32) -> Result<(), BacktestError> + Sync,
    {
        Self::run_simulations_with_limits(
            simulation_count,
            max_concurrent,
            WorkerLimits::global(),
            simulate,
        )
    }

    pub fn run_simulations_with_limits<F>(
        simulation_count: u32,
        max_concurrent: u32,
        limits: &WorkerLimits,
        simulate: F,
    ) -> Result<Vec<Result<(), BacktestError>>, BacktestError>
    where
        F: Fn(u32) -> Result<(), BacktestError> + Sync,
    {
        limits.validate(simulation_count, max_concurrent)?;
        let worker_count = limits.worker_count(simulation_count, max_concurrent);
        let outcomes = SimulationPipeline::run(
            simulation_count,
            worker_count,
            worker_count,
            |index| simulate(index).map(|()| Value::Null),
            |_, _| Ok(()),
        )?;
        Ok(outcomes
            .into_iter()
            .map(|outcome| outcome.result.map(|_| ()))
            .collect())
    }
}
