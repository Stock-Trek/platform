//! Native host binary for the runner.
//!
//! The guest that this binary invokes is the compiled runner itself, which is
//! not able to talk to the network on its own. Instead, it expects a run
//! request on stdin with the following shape:
//!
//! ```json
//! {
//!   "cwasmGetUrl": "<presigned S3 GET url>",
//!   "command": "metadata",
//!   "strategyName": "MyStrategy"
//! }
//! ```
//!
//! The URL points at a runner cwasm artifact built for wasm32-wasip1 (see
//! build-plane for details). This host downloads it, loads it into an
//! embedded Wasmtime engine, provides native implementations of those imports
//! and invokes the algorithm in-process. Every value queued by the algorithm
//! is written to stdout as a single JSON line, e.g.:
//!
//! ```json
//! {"type":"metadata","payload":{"name":"MyStrategy","description":"..."}}
//! ```
//!
//! The algorithm's own stdout and stderr are inherited so that log output
//! shows up in the build runner's logs.

#[path = "../cwasm/mod.rs"]
mod cwasm;

use cwasm::{Cwasm, CwasmLimits, WasmRunner};
use platform_exit_codes::ExitCode;
use platform_runner::command::RunRequest;
use std::io::{self, Read};

struct CwasmInvoker;

impl CwasmInvoker {
    fn run() -> i32 {
        let mut body = String::new();
        if let Err(error) = io::stdin().read_to_string(&mut body) {
            eprintln!("cwasm-invoker: failed to read stdin: {error}");
            return 1;
        }

        let request = match Self::parse_request(&body) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("cwasm-invoker: invalid request: {error}");
                return ExitCode::ServerEntryFailedToParseRequest.code();
            }
        };

        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("cwasm-invoker: failed to start tokio runtime: {error}");
                return ExitCode::ServerRunnerConnectionFailed.code();
            }
        };
        let cwasm = match runtime.block_on(Cwasm::download(
            &request.cwasm_get_url,
            CwasmLimits::default(),
        )) {
            Ok(cwasm) => cwasm,
            Err(error) => {
                eprintln!("cwasm-invoker: failed to download cwasm: {error}");
                return ExitCode::ServerRunnerConnectionFailed.code();
            }
        };
        drop(runtime);

        let runner = match WasmRunner::new() {
            Ok(runner) => runner,
            Err(error) => {
                eprintln!("cwasm-invoker: failed to create wasm engine: {error:?}");
                return ExitCode::ServerRunnerCwasmNotLoaded.code();
            }
        };

        let mut arguments = vec!["runner"];
        arguments.extend(request.command.arguments());
        runner.execute(cwasm.path(), &arguments)
    }

    fn parse_request(body: &str) -> Result<RunRequest, serde_json::Error> {
        serde_json::from_str(body)
    }
}

fn main() {
    std::process::exit(CwasmInvoker::run());
}
