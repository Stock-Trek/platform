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
                "strategyName": "AlgorithmStub",
                "strategyContextPath": "/tmp/strategy-context.bin",
                "portfolioPath": "/tmp/portfolio.bin"
            }"#,
    )
    .unwrap();

    assert_eq!(
        request.cwasm_get_url.as_deref(),
        Some("https://example.com/runner.cwasm")
    );
    assert_eq!(request.command.strategy_name(), "AlgorithmStub");
}

#[test]
fn requests_without_a_cwasm_url_remain_supported() {
    let request: RunRequest = serde_json::from_str(
        r#"{
                "command": "metadata",
                "strategyName": "AlgorithmStub"
            }"#,
    )
    .unwrap();

    assert_eq!(request.cwasm_get_url, None);
    assert_eq!(request.command.strategy_name(), "AlgorithmStub");
}
