use crate::Command;
use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};
use stock_trek::types::{AccountId, AssetId, CexId};

use crate::cwasm::WasmRunner;

use crate::cwasm::backtest::{
    BacktestError, publish::ResultPublisher, simulation::Simulation, spec::Spec,
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
        match Self::run_simulations(
            spec.simulation_count,
            spec.max_concurrent_simulations,
            |index| Self::simulate(runner, cwasm_path, &spec, index),
        )
        .into_iter()
        .find_map(Result::err)
        {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn run_simulations<F>(
        simulation_count: u32,
        max_concurrent: u32,
        simulate: F,
    ) -> Vec<Result<(), BacktestError>>
    where
        F: Fn(u32) -> Result<(), BacktestError> + Sync,
    {
        let worker_count = simulation_count.min(max_concurrent) as usize;
        let next = AtomicU32::new(0);
        let results = Mutex::new(Vec::with_capacity(simulation_count as usize));
        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| {
                    let mut worker_results = Vec::new();
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= simulation_count {
                            break;
                        }
                        let result = catch_unwind(AssertUnwindSafe(|| simulate(index)))
                            .unwrap_or(Err(BacktestError::Panic));
                        worker_results.push((index, result));
                    }
                    results.lock().extend(worker_results);
                });
            }
        });
        let mut results = results.into_inner();
        results.sort_by_key(|(index, _)| *index);
        results.into_iter().map(|(_, result)| result).collect()
    }

    fn simulate(
        runner: &WasmRunner,
        cwasm_path: &Path,
        spec: &Spec,
        index: u32,
    ) -> Result<(), BacktestError> {
        let result = Simulation::run(runner, cwasm_path, spec, index)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(BacktestError::RuntimeStart)?;
        runtime.block_on(ResultPublisher::publish(spec, &result))?;
        Ok(())
    }
}
