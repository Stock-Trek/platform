//! Native host for the `runner` algorithm.
//!
//! `build-agent` spawns this binary, writes the request body to its stdin and
//! forwards its stdout/stderr to the agent logs. The body is a JSON object
//! shaped like:
//!
//! ```json
//! {
//!   "command": "execute",
//!   "strategyName": "AlgorithmStub",
//!   "strategyContextPath": "/tmp/strategy-context.bin",
//!   "portfolioPath": "/tmp/portfolio.bin"
//! }
//! ```
//!
//! The `runner` crate implements an algorithm that imports the `env::queue_*`
//! functions. This host provides native implementations of those imports and
//! invokes the algorithm in-process. Every value queued by the algorithm is
//! written to stdout as a single JSON line:
//!
//! ```text
//! {"type":"strategy","payload":"{...}"}
//! {"type":"metadata","payload":{"name":"...","description":"..."}}
//! ```

use platform_exit_codes::ExitCode;
use platform_runner::{command::Command, entry::Entry};
use serde_json::{Value, json};
use std::{
    ffi::{CString, NulError},
    io::{self, Read},
};

struct BuildRunner;

impl BuildRunner {
    fn run() -> i32 {
        let mut body = String::new();
        if let Err(error) = io::stdin().read_to_string(&mut body) {
            eprintln!("build-runner: failed to read stdin: {error}");
            return 1;
        }

        let command = match Self::parse_request(&body) {
            Ok(command) => command,
            Err(error) => {
                eprintln!("build-runner: invalid request: {error}");
                return ExitCode::ServerEntryFailedToParseRequest.code();
            }
        };

        match Self::invoke(&command) {
            Ok(exit_code) => exit_code,
            Err(error) => {
                eprintln!("build-runner: invalid argument: {error}");
                ExitCode::ServerEntryInvalidArgument.code()
            }
        }
    }

    /// Deserialise the request body into a `Command`.
    fn parse_request(body: &str) -> Result<Command, String> {
        if body.trim().is_empty() {
            return Err("body is empty".to_string());
        }
        serde_json::from_str(body)
            .map_err(|error| format!("body is not a valid run request: {error}"))
    }

    /// Invoke the algorithm entry point with the resolved arguments.
    fn invoke(command: &Command) -> Result<i32, NulError> {
        let args = Self::args(command)
            .iter()
            .map(|arg| Self::cstring(arg))
            .collect::<Result<Vec<_>, _>>()?;
        let argv = args.iter().map(|arg| arg.as_ptr()).collect::<Vec<_>>();

        // Safety: `argv` holds valid, NUL-terminated C strings for the duration of
        // the call and its length matches the number of arguments passed to `entry`.
        Ok(unsafe { Entry::run(argv.len() as i32, argv.as_ptr()) })
    }

    /// Build the argv layout expected by `entry` for `command`.
    fn args(command: &Command) -> Vec<&str> {
        match command {
            Command::Algorithm { strategy_name } => {
                vec!["build-runner", "Algorithm", strategy_name.as_str()]
            }
            Command::Execute {
                strategy_name,
                strategy_context_path,
                portfolio_path,
            } => vec![
                "build-runner",
                "Execute",
                strategy_name.as_str(),
                strategy_context_path.as_str(),
                portfolio_path.as_str(),
            ],
            Command::Metadata { strategy_name } => {
                vec!["build-runner", "Metadata", strategy_name.as_str()]
            }
            Command::Preferences { strategy_name } => {
                vec!["build-runner", "Preferences", strategy_name.as_str()]
            }
            Command::Signals {
                strategy_name,
                strategy_context_path,
            } => vec![
                "build-runner",
                "Signals",
                strategy_name.as_str(),
                strategy_context_path.as_str(),
            ],
            // `Command` is `#[non_exhaustive]`.
            _ => vec!["build-runner", "Algorithm", ""],
        }
    }

    fn cstring(value: &str) -> Result<CString, NulError> {
        CString::new(value)
    }

    /// Borrow a `(pointer, length)` pair as a byte slice.
    ///
    /// # Safety
    /// When `length` is non-zero, `pointer` must be valid for reads of `length`
    /// bytes for the duration of the borrow.
    unsafe fn bytes<'a>(pointer: *const u8, length: usize) -> &'a [u8] {
        if pointer.is_null() || length == 0 {
            &[]
        } else {
            // Safety: upheld by the caller.
            unsafe { std::slice::from_raw_parts(pointer, length) }
        }
    }

    /// Print a queued value as a JSON line.
    fn emit(kind: &str, payload: Value) {
        println!("{}", json!({ "type": kind, "payload": payload }));
    }

    unsafe fn queue_metadata(
        name_ptr: *const u8,
        name_len: usize,
        description_ptr: *const u8,
        description_len: usize,
    ) {
        // Safety: the ABI guarantees the pointer/length pairs are valid.
        let name = unsafe { Self::bytes(name_ptr, name_len) };
        let description = unsafe { Self::bytes(description_ptr, description_len) };
        Self::emit(
            "metadata",
            json!({
                "name": String::from_utf8_lossy(name),
                "description": String::from_utf8_lossy(description),
            }),
        );
    }

    unsafe fn queue_preferences(preferences_ptr: *const u8, preferences_len: usize) {
        // Safety: the ABI guarantees the pointer/length pair is valid.
        let preferences = unsafe { Self::bytes(preferences_ptr, preferences_len) };
        Self::emit("preferences", json!(String::from_utf8_lossy(preferences)));
    }

    unsafe fn queue_signals(signals_ptr: *const u8, signals_len: usize) {
        // Safety: the ABI guarantees the pointer/length pair is valid.
        let signals = unsafe { Self::bytes(signals_ptr, signals_len) };
        Self::emit("signals", json!(String::from_utf8_lossy(signals)));
    }

    unsafe fn queue_strategy(strategy_ptr: *const u8, strategy_len: usize) {
        // Safety: the ABI guarantees the pointer/length pair is valid.
        let strategy = unsafe { Self::bytes(strategy_ptr, strategy_len) };
        Self::emit("strategy", json!(String::from_utf8_lossy(strategy)));
    }

    unsafe fn queue_action(
        action_ptr: *const u8,
        action_len: usize,
        recovery_policy_ptr: *const u8,
        recovery_policy_len: usize,
    ) {
        // Safety: the ABI guarantees the pointer/length pairs are valid.
        let action = unsafe { Self::bytes(action_ptr, action_len) };
        let recovery_policy = unsafe { Self::bytes(recovery_policy_ptr, recovery_policy_len) };
        Self::emit(
            "action",
            json!({
                "action": String::from_utf8_lossy(action),
                "recoveryPolicy": String::from_utf8_lossy(recovery_policy),
            }),
        );
    }
}

fn main() {
    std::process::exit(BuildRunner::run());
}

#[unsafe(no_mangle)]
unsafe extern "C" fn queue_metadata(
    name_ptr: *const u8,
    name_len: usize,
    description_ptr: *const u8,
    description_len: usize,
) {
    unsafe {
        BuildRunner::queue_metadata(name_ptr, name_len, description_ptr, description_len);
    }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn queue_preferences(preferences_ptr: *const u8, preferences_len: usize) {
    unsafe { BuildRunner::queue_preferences(preferences_ptr, preferences_len) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn queue_signals(signals_ptr: *const u8, signals_len: usize) {
    unsafe { BuildRunner::queue_signals(signals_ptr, signals_len) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn queue_strategy(strategy_ptr: *const u8, strategy_len: usize) {
    unsafe { BuildRunner::queue_strategy(strategy_ptr, strategy_len) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn queue_action(
    action_ptr: *const u8,
    action_len: usize,
    recovery_policy_ptr: *const u8,
    recovery_policy_len: usize,
) {
    unsafe {
        BuildRunner::queue_action(
            action_ptr,
            action_len,
            recovery_policy_ptr,
            recovery_policy_len,
        );
    }
}
