use super::*;
use platform_exit_codes::ExitCode;
use platform_runner::command::Command;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::NamedTempFile;

const METADATA_WAT: &str = r#"
    (module
        (import "env" "queue_metadata" (func $queue_metadata (param i32 i32 i32 i32)))
        (memory (export "memory") 1)
        (data (i32.const 64) "Metadata")
        (global $next (mut i32) (i32.const 8192))
        (func (export "alloc") (param $size i32) (result i32)
            (local $pointer i32)
            (local.set $pointer (global.get $next))
            (global.set $next (i32.add (global.get $next) (local.get $size)))
            (local.get $pointer))
        (func $strlen (param $pointer i32) (result i32)
            (local $length i32)
            (block
                (loop
                    (br_if 1 (i32.eqz (i32.load8_u
                        (i32.add (local.get $pointer) (local.get $length)))))
                    (local.set $length (i32.add (local.get $length) (i32.const 1)))
                    (br 0)))
            (local.get $length))
        (func (export "entry") (param $argc i32) (param $argv i32) (result i32)
            (local $pointer i32)
            (local.set $pointer (i32.load (i32.add (local.get $argv) (i32.const 4))))
            (call $queue_metadata
                (local.get $pointer)
                (call $strlen (local.get $pointer))
                (i32.const 64)
                (i32.const 8))
            (i32.const 0)))"#;

const EXIT_WAT: &str = r#"
    (module
        (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) (i32.const 8192))
        (func (export "entry") (param i32 i32) (result i32)
            (call $proc_exit (i32.const 42))
            (i32.const 0)))"#;

const TRAP_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) (i32.const 8192))
        (func (export "entry") (param i32 i32) (result i32)
            (unreachable)))"#;

const LOOP_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) (i32.const 8192))
        (func (export "entry") (param i32 i32) (result i32)
            (loop $forever
                (br $forever))
            (i32.const 0)))"#;

const GROW_MEMORY_WAT: &str = r#"
    (module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) (i32.const 8192))
        (func (export "entry") (param i32 i32) (result i32)
            (drop (memory.grow (i32.const 1024)))
            (i32.const 0)))"#;

const OPEN_HOST_FILE_WAT: &str = r#"
    (module
        (import "wasi_snapshot_preview1" "path_open"
            (func $path_open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 64) "/proc/self/environ")
        (func (export "alloc") (param i32) (result i32) (i32.const 8192))
        (func (export "entry") (param i32 i32) (result i32)
            (if (i32.eqz
                    (call $path_open
                        (i32.const 3)
                        (i32.const 0)
                        (i32.const 64)
                        (i32.const 18)
                        (i32.const 0)
                        (i64.const 0)
                        (i64.const 0)
                        (i32.const 0)
                        (i32.const 128)))
                (then (return (i32.const 99))))
            (i32.const 0)))"#;

type CapturedLines = Arc<Mutex<Vec<(String, Value)>>>;

fn artifact(wat: &str) -> NamedTempFile {
    let engine = wasmtime::Engine::new(&execution_config()).unwrap();
    let module = wasmtime::Module::new(&engine, wat).unwrap();
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), module.serialize().unwrap()).unwrap();
    file
}

fn capture() -> (Emit, CapturedLines) {
    let lines: CapturedLines = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&lines);
    let emit: Emit = Arc::new(move |kind, payload| {
        sink.lock().unwrap().push((kind.to_string(), payload));
    });
    (emit, lines)
}

#[test]
fn runs_the_guest_entry_with_host_written_arguments() {
    let runner = WasmRunner::new().unwrap();
    let file = artifact(METADATA_WAT);
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner", "Metadata", "unused"], &emit);

    assert_eq!(code, ExitCode::Success.code());
    let lines = lines.lock().unwrap();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].0, "metadata");
    assert_eq!(
        lines[0].1,
        json!({"name": "Metadata", "description": "Metadata"})
    );
}

#[test]
fn passes_the_guest_exit_code_through() {
    let runner = WasmRunner::new().unwrap();
    let file = artifact(EXIT_WAT);
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner"], &emit);

    assert_eq!(code, 42);
    assert!(lines.lock().unwrap().is_empty());
}

#[test]
fn reports_a_trapped_guest_as_a_runner_panic() {
    let runner = WasmRunner::new().unwrap();
    let file = artifact(TRAP_WAT);
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner"], &emit);

    assert_eq!(code, ExitCode::BotErrorPanic.code());
    assert!(lines.lock().unwrap().is_empty());
}

#[test]
fn reports_an_invalid_artifact_as_not_loaded() {
    let runner = WasmRunner::new().unwrap();
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"not a cwasm artifact").unwrap();
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner"], &emit);

    assert_eq!(code, ExitCode::ServerRunnerCwasmNotLoaded.code());
    assert!(lines.lock().unwrap().is_empty());
}

#[test]
fn interrupts_a_guest_that_exceeds_its_deadline() {
    let runner = WasmRunner::with_limits(ExecutionLimits::new(
        Duration::from_millis(100),
        DEFAULT_MAX_GUEST_MEMORY_BYTES,
    ))
    .unwrap();
    let file = artifact(LOOP_WAT);
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner"], &emit);

    assert_eq!(code, ExitCode::BotErrorTimeout.code());
    assert!(lines.lock().unwrap().is_empty());
}

#[test]
fn interrupts_a_guest_that_exceeds_its_memory_limit() {
    let runner =
        WasmRunner::with_limits(ExecutionLimits::new(Duration::from_secs(5), 128 * 1024)).unwrap();
    let file = artifact(GROW_MEMORY_WAT);
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner"], &emit);

    assert_eq!(code, ExitCode::BotErrorResourceLimitExceeded.code());
    assert!(lines.lock().unwrap().is_empty());
}

#[test]
fn does_not_expose_host_files_outside_the_guest_workspace() {
    let runner = WasmRunner::new().unwrap();
    let file = artifact(OPEN_HOST_FILE_WAT);
    let (emit, lines) = capture();

    let code = runner.execute_with(file.path(), &["runner"], &emit);

    assert_eq!(code, ExitCode::Success.code());
    assert!(lines.lock().unwrap().is_empty());
}

#[test]
fn copies_declared_inputs_into_the_guest_workspace() {
    let source = tempfile::tempdir().unwrap();
    let strategy_context = source.path().join("strategy-context.bin");
    let portfolio = source.path().join("portfolio.bin");
    std::fs::write(&strategy_context, b"context").unwrap();
    std::fs::write(&portfolio, b"portfolio").unwrap();

    let workspace = GuestWorkspace::for_command(&Command::Execute {
        strategy_name: "AlgorithmStub".to_string(),
        strategy_context_path: strategy_context.display().to_string(),
        portfolio_path: portfolio.display().to_string(),
    })
    .unwrap();

    assert_eq!(
        workspace.arguments(),
        &[
            "Execute".to_string(),
            "AlgorithmStub".to_string(),
            "/strategy-context.bin".to_string(),
            "/portfolio.bin".to_string()
        ]
    );
    assert_eq!(
        std::fs::read(workspace.path().join(STRATEGY_CONTEXT_FILE_NAME)).unwrap(),
        b"context"
    );
    assert_eq!(
        std::fs::read(workspace.path().join(PORTFOLIO_FILE_NAME)).unwrap(),
        b"portfolio"
    );
    assert!(!workspace.path().join("strategy-context.bin").is_symlink());
}

#[test]
fn rejects_missing_guest_inputs() {
    let workspace = GuestWorkspace::for_command(&Command::Signals {
        strategy_name: "AlgorithmStub".to_string(),
        strategy_context_path: "/does/not/exist".to_string(),
    });

    assert!(matches!(
        workspace,
        Err(ExitCode::ServerRunnerStrategyContextFileNotFound)
    ));
}
