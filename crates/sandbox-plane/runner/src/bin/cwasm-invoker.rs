//! Native host binary for the runner.
//!
//! The guest that this binary invokes is the compiled runner itself, which is
//! not able to talk to the network on its own. Instead, it expects a run
//! request on stdin with the following shape:
//!
//! ```json
//! {
//!   "cwasmGetUrl": "<presigned S3 GET url>",
//!   "cwasmSha256": "<artifact sha256>",
//!   "cwasmSize": 123456,
//!   "wasmtimeVersion": "43.0.1",
//!   "orderRequestUrl": "<order request service url>",
//!   "owner": "<algorithm owner>",
//!   "command": "metadata",
//!   "strategyName": "MyStrategy"
//! }
//! ```
//!
//! The URL points at a runner cwasm artifact built for wasm32-wasip1 (see
//! build-plane for details). This host downloads it, verifies the pinned
//! SHA-256, size and Wasmtime version, loads it into an embedded Wasmtime
//! engine, provides native implementations of those imports and invokes the
//! algorithm in-process. Every value queued by the algorithm is written to
//! stdout as a single JSON line, e.g.:
//!
//! ```json
//! {"type":"metadata","payload":{"name":"MyStrategy","description":"..."}}
//! ```
//!
//! The algorithm's own stdout and stderr are routed to stderr so guest output
//! cannot spoof or corrupt the structured result stream.

#[path = "../cwasm/mod.rs"]
mod cwasm;

use aws_sdk_sqs::Client as SqsClient;
use chrono::Duration;
use cwasm::{
    AuditConfig, Cwasm, CwasmIntegrity, GuestActions, InvokerConfig, OrderRequestDispatcher,
    WasmRunner, backtest,
};
use platform_auth_service::{
    AuthService, AuthServiceConfig, MemoryReplayStore, SqsTokenAuditLog, VaultTokenStore,
    VaultTokenStoreConfig,
};
use platform_aws::Aws;
use platform_exit_codes::ExitCode;
use platform_runner::command::{Command, RunRequest};
use platform_sqs::sqs::{Sqs, SqsConfig};
use platform_time::timeouts::Timeouts;
use std::{
    io::{self, Read},
    sync::Arc,
};

const MAX_RUN_REQUEST_BYTES: u64 = 1024 * 1024;

struct CwasmInvoker;

impl CwasmInvoker {
    fn run() -> i32 {
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
            vault,
            audit,
        } = config;
        let runtime = match Self::runtime() {
            Ok(runtime) => runtime,
            Err(code) => return code.code(),
        };
        let auth_service = match runtime.block_on(Self::auth_service(vault, audit)) {
            Ok(auth_service) => auth_service,
            Err(code) => return code.code(),
        };
        let dispatcher = match OrderRequestDispatcher::new(
            &request.order_request_url,
            &request.owner,
            auth_service,
        ) {
            Ok(dispatcher) => dispatcher,
            Err(error) => {
                eprintln!("cwasm-invoker: invalid order request configuration: {error}");
                return ExitCode::ServerRunnerConfigurationInvalid.code();
            }
        };
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

        if matches!(request.command, Command::BackTest { .. }) {
            return backtest::BackTest::run(&runner, cwasm.path(), &request.command);
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

    async fn auth_service(
        config: VaultTokenStoreConfig,
        audit: AuditConfig,
    ) -> Result<AuthService, ExitCode> {
        let store = VaultTokenStore::new(config).map_err(|error| {
            eprintln!("cwasm-invoker: invalid auth service configuration: {error}");
            ExitCode::ServerRunnerConfigurationInvalid
        })?;
        let timeouts = Timeouts::from_durations(audit.timeout, audit.timeout);
        let sdk_config = Aws::load_sdk_config(timeouts).await;
        let sqs = Sqs::new(
            SqsClient::new(&sdk_config),
            timeouts,
            SqsConfig::new(audit.queue_url),
        );
        let audit = SqsTokenAuditLog::new(sqs, audit.timeout);
        AuthService::new(
            Arc::new(store),
            AuthServiceConfig {
                token_lifetime: Duration::days(30),
                rotation_overlap: Duration::days(7),
                signature_validity: Duration::minutes(5),
                audit: Arc::new(audit),
            },
            Arc::new(MemoryReplayStore::default()),
        )
        .map_err(|error| {
            eprintln!("cwasm-invoker: invalid auth service configuration: {error}");
            ExitCode::ServerRunnerConfigurationInvalid
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

fn main() {
    std::process::exit(CwasmInvoker::run());
}
