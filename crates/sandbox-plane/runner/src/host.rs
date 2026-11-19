use crate::cwasm::{
    Cwasm, CwasmIntegrity, GuestActions, InvokerConfig, OrderRequestDispatcher, WasmRunner,
    backtest,
};
use crate::{Command, RunRequest};
use platform_exit_codes::ExitCode;
use std::io::{self, Read};

const MAX_RUN_REQUEST_BYTES: u64 = 1024 * 1024;

pub struct CwasmInvoker;

impl CwasmInvoker {
    pub fn run() -> i32 {
        let body = match Self::read_request() {
            Ok(body) => body,
            Err(code) => return code.code(),
        };

        let request = match Self::parse_request(&body) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("cwasm-invoker: invalid request: {error}");
                return ExitCode::ServerEntryFailedToParseRequest.code();
            }
        };

        let config = match InvokerConfig::from_env() {
            Ok(config) => config,
            Err(error) => {
                eprintln!("cwasm-invoker: invalid configuration: {error}");
                return ExitCode::ServerRunnerConfigurationInvalid.code();
            }
        };

        let InvokerConfig {
            execution,
            cwasm: cwasm_limits,
            sigv4,
        } = config;
        let runtime = match Self::runtime() {
            Ok(runtime) => runtime,
            Err(code) => return code.code(),
        };
        let dispatcher = match OrderRequestDispatcher::with_sigv4(
            &request.order_request_url,
            &request.auth_service_url,
            &request.owner,
            &sigv4,
        ) {
            Ok(dispatcher) => dispatcher,
            Err(error) => {
                eprintln!("cwasm-invoker: invalid order request configuration: {error}");
                return ExitCode::ServerRunnerConfigurationInvalid.code();
            }
        };
        if let Err(error) = runtime.block_on(dispatcher.prime()) {
            eprintln!("cwasm-invoker: failed to fetch an order request JWT: {error}");
            return ExitCode::ServerRunnerConnectionFailed.code();
        }
        let integrity = CwasmIntegrity::new(
            request.cwasm_sha256.clone(),
            request.cwasm_size,
            request.wasmtime_version.clone(),
        );
        let cwasm = match runtime.block_on(Cwasm::download(
            &request.cwasm_get_url,
            &integrity,
            cwasm_limits,
        )) {
            Ok(cwasm) => cwasm,
            Err(error) => {
                eprintln!("cwasm-invoker: failed to download cwasm: {error}");
                return ExitCode::ServerRunnerCwasmDownloadFailed.code();
            }
        };

        let runner = match WasmRunner::with_limits(execution) {
            Ok(runner) => runner,
            Err(error) => {
                eprintln!("cwasm-invoker: failed to create wasm engine: {error:?}");
                return ExitCode::ServerRunnerCwasmNotLoaded.code();
            }
        };

        if matches!(request.command, Command::Backtest { .. }) {
            return match backtest::Backtest::run(&runner, cwasm.path(), &request.command) {
                Ok(()) => ExitCode::Success.code(),
                Err(error) => {
                    eprintln!("cwasm-invoker: {error}");
                    error.exit_code()
                }
            };
        }

        let actions = GuestActions::new();
        let code = runner.execute_command_with(cwasm.path(), &request.command, &actions.emit());
        if code != ExitCode::Success.code() {
            return code;
        }

        match runtime.block_on(dispatcher.dispatch(actions.take())) {
            Ok(()) => ExitCode::Success.code(),
            Err(error) => {
                eprintln!("cwasm-invoker: failed to submit order requests: {error}");
                ExitCode::ServerRunnerConnectionFailed.code()
            }
        }
    }

    fn runtime() -> Result<tokio::runtime::Runtime, ExitCode> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                eprintln!("cwasm-invoker: failed to start tokio runtime: {error}");
                ExitCode::ServerRunnerRuntimeStartFailed
            })
    }

    fn read_request() -> Result<String, ExitCode> {
        let mut body = Vec::new();
        io::stdin()
            .take(MAX_RUN_REQUEST_BYTES + 1)
            .read_to_end(&mut body)
            .map_err(|error| {
                eprintln!("cwasm-invoker: failed to read stdin: {error}");
                ExitCode::ServerEntryFailedToParseRequest
            })?;
        if body.len() as u64 > MAX_RUN_REQUEST_BYTES {
            eprintln!(
                "cwasm-invoker: request exceeds the {MAX_RUN_REQUEST_BYTES}-byte stdin limit"
            );
            return Err(ExitCode::ServerEntryFailedToParseRequest);
        }
        String::from_utf8(body).map_err(|error| {
            eprintln!("cwasm-invoker: request is not valid UTF-8: {error}");
            ExitCode::ServerEntryFailedToParseRequest
        })
    }

    fn parse_request(body: &str) -> Result<RunRequest, serde_json::Error> {
        serde_json::from_str(body)
    }
}
