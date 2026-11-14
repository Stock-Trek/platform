use serde_json::Value;
use std::{collections::BTreeMap, path::Path};
use stock_trek::types::{AccountId, AssetId, CexId};

use crate::Command;
use crate::cwasm::WasmRunner;

use crate::cwasm::backtest::{
    BacktestError, pipeline::PublisherPipeline, publish::ResultPublisher, simulation::Simulation,
    spec::Spec, worker_limits::WorkerLimits,
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
        let results = PublisherPipeline::run(
            spec.simulation_count,
            worker_count,
            worker_count,
            |index| Simulation::run(runner, cwasm_path, &spec, index),
            |runtime, _, result| runtime.block_on(ResultPublisher::publish(&spec, result)),
        )?;
        match results.into_iter().find_map(Result::err) {
            Some(error) => Err(error),
            None => Ok(()),
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
        PublisherPipeline::run(
            simulation_count,
            worker_count,
            worker_count,
            |index| simulate(index).map(|()| Value::Null),
            |_, _, _| Ok(()),
        )
    }
}
