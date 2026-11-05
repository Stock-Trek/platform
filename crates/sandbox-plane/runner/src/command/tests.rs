use super::*;

#[test]
fn deserialises_camel_case_command_payloads() {
    let command: Command = serde_json::from_str(
        r#"{
                "command": "execute",
                "strategyName": "AlgorithmStub",
                "strategyContextPath": "/tmp/strategy-context.bin",
                "portfolioPath": "/tmp/portfolio.bin"
            }"#,
    )
    .unwrap();

    assert_eq!(
        command,
        Command::Execute {
            strategy_name: "AlgorithmStub".to_string(),
            strategy_context_path: "/tmp/strategy-context.bin".to_string(),
            portfolio_path: "/tmp/portfolio.bin".to_string(),
        }
    );
}

#[test]
fn deserialises_the_cwasm_url_alongside_the_command() {
    let request: RunRequest = serde_json::from_str(
        r#"{
                "command": "execute",
                "cwasmGetUrl": "https://example.com/runner.cwasm",
                "cwasmSha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "cwasmSize": 8,
                "wasmtimeVersion": "43.0.1",
                "strategyName": "AlgorithmStub",
                "strategyContextPath": "/tmp/strategy-context.bin",
                "portfolioPath": "/tmp/portfolio.bin"
            }"#,
    )
    .unwrap();

    assert_eq!(request.cwasm_get_url, "https://example.com/runner.cwasm");
    assert_eq!(
        request.cwasm_sha256,
        "0000000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(request.cwasm_size, 8);
    assert_eq!(request.wasmtime_version, "43.0.1");
    assert_eq!(request.command.strategy_name(), "AlgorithmStub");
}

#[test]
fn rejects_requests_without_a_cwasm_url() {
    let result = serde_json::from_str::<RunRequest>(
        r#"{
                "command": "metadata",
                "cwasmSha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "cwasmSize": 8,
                "wasmtimeVersion": "43.0.1",
                "strategyName": "AlgorithmStub"
            }"#,
    );

    assert!(result.is_err());
}

#[test]
fn rejects_requests_without_cwasm_integrity_metadata() {
    let result = serde_json::from_str::<RunRequest>(
        r#"{
                "command": "metadata",
                "cwasmGetUrl": "https://example.com/runner.cwasm",
                "strategyName": "AlgorithmStub"
            }"#,
    );

    assert!(result.is_err());
}

#[test]
fn builds_entry_arguments_for_every_command() {
    assert_eq!(
        Command::Algorithm {
            strategy_name: "AlgorithmStub".to_string(),
        }
        .arguments(),
        vec!["Algorithm", "AlgorithmStub"]
    );
    assert_eq!(
        Command::Execute {
            strategy_name: "AlgorithmStub".to_string(),
            strategy_context_path: "/tmp/strategy-context.bin".to_string(),
            portfolio_path: "/tmp/portfolio.bin".to_string(),
        }
        .arguments(),
        vec![
            "Execute",
            "AlgorithmStub",
            "/tmp/strategy-context.bin",
            "/tmp/portfolio.bin"
        ]
    );
    assert_eq!(
        Command::Metadata {
            strategy_name: "AlgorithmStub".to_string(),
        }
        .arguments(),
        vec!["Metadata", "AlgorithmStub"]
    );
    assert_eq!(
        Command::Preferences {
            strategy_name: "AlgorithmStub".to_string(),
        }
        .arguments(),
        vec!["Preferences", "AlgorithmStub"]
    );
    assert_eq!(
        Command::Signals {
            strategy_name: "AlgorithmStub".to_string(),
            strategy_context_path: "/tmp/strategy-context.bin".to_string(),
        }
        .arguments(),
        vec!["Signals", "AlgorithmStub", "/tmp/strategy-context.bin"]
    );
}
