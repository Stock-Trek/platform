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
