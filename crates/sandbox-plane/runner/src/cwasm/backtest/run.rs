use platform_exit_codes::ExitCode;
use platform_runner::command::Command;
use std::{collections::BTreeMap, path::Path};
use stock_trek::types::{AccountId, AssetId, CexId};

use crate::cwasm::WasmRunner;

use super::{publish::publish, simulation::run_simulation, spec::Spec};

pub(super) type Balances = BTreeMap<CexId, BTreeMap<AccountId, BTreeMap<AssetId, f64>>>;
pub(super) type PriceMap = BTreeMap<(CexId, AssetId, AssetId), f64>;

pub fn run(runner: &WasmRunner, cwasm_path: &Path, command: &Command) -> i32 {
    let spec = match Spec::new(command) {
        Ok(spec) => spec,
        Err(code) => return code,
    };
    let results = std::thread::scope(|scope| {
        (0..spec.simulation_count)
            .map(|index| {
                let spec = &spec;
                scope.spawn(move || simulate(runner, cwasm_path, spec, index))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(ExitCode::BotErrorPanic.code()))
            })
            .collect::<Vec<_>>()
    });
    results
        .into_iter()
        .find_map(Result::err)
        .unwrap_or(ExitCode::Success.code())
}

fn simulate(runner: &WasmRunner, cwasm_path: &Path, spec: &Spec, index: u32) -> Result<(), i32> {
    let result = run_simulation(runner, cwasm_path, spec, index)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            eprintln!("cwasm-invoker: failed to start tokio runtime: {error}");
            ExitCode::ServerRunnerRuntimeStartFailed.code()
        })?;
    runtime.block_on(publish(spec, &result));
    Ok(())
}
