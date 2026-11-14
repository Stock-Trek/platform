use parking_lot::Mutex;
use platform_exit_codes::ExitCode;
use prost::Message;
use rand::{SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use std::{fs, path::Path, sync::Arc};
use stock_trek::actions::ResolvedAction;

use crate::cwasm::{Emit, PORTFOLIO_FILE_NAME, STRATEGY_CONTEXT_FILE_NAME, WasmRunner};

use crate::cwasm::backtest::{
    BacktestError, Balances, PlatformResult, PriceMap, orders::OrderProcessor,
    portfolio::Portfolio, spec::Spec,
};

const GUEST_ROOT: &str = "/";
const PROGRAM_NAME: &str = "cwasm-invoker";

type Emitted = Arc<Mutex<Vec<(String, Value)>>>;

pub struct Simulation;

impl Simulation {
    pub fn run(
        runner: &WasmRunner,
        cwasm_path: &Path,
        spec: &Spec,
        index: u32,
    ) -> PlatformResult<Value> {
        let mut rng = StdRng::seed_from_u64(spec.random_seed.wrapping_add(u64::from(index)));
        let mut balances = spec.initial_balances.clone();
        let directory = tempfile::Builder::new()
            .prefix("platform-backtest-")
            .tempdir()
            .map_err(BacktestError::ScratchPad)?;
        let mut steps = Vec::new();
        for step in 0..spec.steps {
            let (context, prices, timestamp) = spec.step_context(step, &mut rng)?;
            let portfolio = Portfolio::portfolio_dto(&balances);
            Self::write_guest_input(directory.path(), STRATEGY_CONTEXT_FILE_NAME, &context)?;
            Self::write_guest_input(directory.path(), PORTFOLIO_FILE_NAME, &portfolio)?;
            let events = Self::execute_step(
                runner,
                cwasm_path,
                spec,
                directory.path(),
                &mut balances,
                &prices,
                &mut rng,
            )?;
            steps.push(json!({
                "timestamp": timestamp,
                "assetTotals": Portfolio::asset_totals(&balances),
                "events": events,
            }));
        }
        Ok(json!({
            "userId": spec.user_id,
            "algorithmId": spec.algorithm_id,
            "backTestId": spec.back_test_id,
            "simulationId": format!("{}-{index}", spec.back_test_id),
            "simulationSeed": spec.random_seed.wrapping_add(u64::from(index)),
            "initialAssetTotals": Portfolio::asset_totals(&spec.initial_balances),
            "steps": steps,
        }))
    }

    fn write_guest_input<T: Message>(
        directory: &Path,
        name: &str,
        message: &T,
    ) -> PlatformResult<()> {
        let path = directory.join(name);
        Ok(fs::write(&path, message.encode_to_vec())
            .map_err(|source| BacktestError::WriteGuestInput { path, source })?)
    }

    fn execute_step(
        runner: &WasmRunner,
        cwasm_path: &Path,
        spec: &Spec,
        directory: &Path,
        balances: &mut Balances,
        prices: &PriceMap,
        rng: &mut StdRng,
    ) -> PlatformResult<Vec<Value>> {
        let (emit, collected) = Self::collector();
        let arguments = [
            PROGRAM_NAME,
            "Execute",
            spec.strategy_name.as_str(),
            &Self::guest_path(STRATEGY_CONTEXT_FILE_NAME),
            &Self::guest_path(PORTFOLIO_FILE_NAME),
        ];
        let code = runner.execute_arguments(cwasm_path, &arguments, directory, &emit);
        if code != ExitCode::Success.code() {
            return Err(BacktestError::GuestStepFailed(code).into());
        }
        let lines = std::mem::take(&mut *collected.lock());
        let mut events = Vec::new();
        for (kind, payload) in lines {
            if kind != "action" {
                continue;
            }
            let Some(action_json) = payload.get("action").and_then(Value::as_str) else {
                return Err(BacktestError::MissingAction.into());
            };
            let action: ResolvedAction =
                serde_json::from_str(action_json).map_err(BacktestError::InvalidAction)?;
            events.push(OrderProcessor::handle_action(
                &action,
                balances,
                prices,
                rng,
                &spec.simulation,
            )?);
        }
        Ok(events)
    }

    fn collector() -> (Emit, Emitted) {
        let lines: Emitted = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        let emit: Emit = Arc::new(move |kind, payload| {
            sink.lock().push((kind.to_string(), payload));
        });
        (emit, lines)
    }

    fn guest_path(name: &str) -> String {
        format!("{GUEST_ROOT}{name}")
    }
}
