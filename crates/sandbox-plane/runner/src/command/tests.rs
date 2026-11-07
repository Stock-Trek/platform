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
                "orderRequestUrl": "https://orders.example.com/requests",
                "owner": "demo-owner",
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
    assert_eq!(
        request.order_request_url,
        "https://orders.example.com/requests"
    );
    assert_eq!(request.owner, "demo-owner");
    assert_eq!(request.command.strategy_name(), "AlgorithmStub");
}

#[test]
fn rejects_requests_without_an_order_request_url() {
    let result = serde_json::from_str::<RunRequest>(
        r#"{
                "command": "metadata",
                "cwasmGetUrl": "https://example.com/runner.cwasm",
                "cwasmSha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "cwasmSize": 8,
                "wasmtimeVersion": "43.0.1",
                "strategyName": "AlgorithmStub"
            }"#,
    );

    assert!(result.is_err());
}

#[test]
fn rejects_requests_without_an_owner() {
    let result = serde_json::from_str::<RunRequest>(
        r#"{
                "command": "metadata",
                "cwasmGetUrl": "https://example.com/runner.cwasm",
                "cwasmSha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "cwasmSize": 8,
                "wasmtimeVersion": "43.0.1",
                "orderRequestUrl": "https://orders.example.com/requests",
                "strategyName": "AlgorithmStub"
            }"#,
    );

    assert!(result.is_err());
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

#[test]
fn deserialises_the_back_test_command() {
    let command: Command = serde_json::from_str(
        r#"{
                "command": "backTest",
                "strategyName": "AlgorithmStub",
                "strategyContextPath": "/tmp/strategy-context.bin",
                "portfolioPath": "/tmp/portfolio.bin",
                "marketDataPath": "/tmp/market-data.bin",
                "resultsStreamUrl": "wss://example.com/results",
                "resultsPostUrl": "https://example.com/results",
                "userId": "user-1",
                "algorithmId": "algorithm-1",
                "backTestId": "back-test-1",
                "simulationCount": 10,
                "randomSeed": 42,
                "tradingPairs": [{"base": "Bitcoin", "quote": "TetherUSD"}],
                "windows": [5, 12],
                "historicalCandleCount": 300,
                "openCandleProgressMillis": 30000
            }"#,
    )
    .unwrap();

    assert_eq!(command.strategy_name(), "AlgorithmStub");
    assert_eq!(command.arguments(), vec!["BackTest", "AlgorithmStub"]);
    let Command::BackTest {
        simulation_count,
        random_seed,
        trading_pairs,
        windows,
        historical_candle_count,
        open_candle_progress_millis,
        simulation,
        ..
    } = &command
    else {
        panic!("expected a back test command");
    };
    assert_eq!(*simulation_count, 10);
    assert_eq!(*random_seed, 42);
    assert_eq!(
        trading_pairs,
        &[TradingPair {
            base: "Bitcoin".to_string(),
            quote: "TetherUSD".to_string(),
        }]
    );
    assert_eq!(windows, &[5, 12]);
    assert_eq!(*historical_candle_count, 300);
    assert_eq!(*open_candle_progress_millis, 30000);
    assert_eq!(*simulation, SimulationParameters::default());
}

#[test]
fn deserialises_back_test_simulation_parameters() {
    let command: Command = serde_json::from_str(
        r#"{
                "command": "backTest",
                "strategyName": "AlgorithmStub",
                "strategyContextPath": "/tmp/strategy-context.bin",
                "portfolioPath": "/tmp/portfolio.bin",
                "marketDataPath": "/tmp/market-data.bin",
                "resultsStreamUrl": "wss://example.com/results",
                "resultsPostUrl": "https://example.com/results",
                "userId": "user-1",
                "algorithmId": "algorithm-1",
                "backTestId": "back-test-1",
                "simulationCount": 1,
                "randomSeed": 42,
                "tradingPairs": [{"base": "Bitcoin", "quote": "TetherUSD"}],
                "windows": [5],
                "historicalCandleCount": 300,
                "openCandleProgressMillis": 30000,
                "simulation": {
                    "candleLossChance": 0.01,
                    "slippageChance": 0.1,
                    "slippageProportion": 0.002,
                    "lostOrderChance": 0.02
                }
            }"#,
    )
    .unwrap();

    let Command::BackTest { simulation, .. } = &command else {
        panic!("expected a back test command");
    };
    assert_eq!(
        simulation,
        &SimulationParameters {
            candle_loss_chance: 0.01,
            slippage_chance: 0.1,
            slippage_proportion: 0.002,
            lost_order_chance: 0.02,
        }
    );
}
