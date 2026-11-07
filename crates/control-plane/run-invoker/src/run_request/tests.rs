use super::{RunCommand, RunRequest};
use chrono::{DateTime, Utc};
use platform_aws::presigned_post::PresignedPost;
use platform_run_store::{CwasmArtifact, RunRecord};
use std::collections::BTreeMap;

fn record() -> RunRecord {
    RunRecord::new(
        "demo-algorithm",
        "demo-owner",
        DateTime::parse_from_rfc3339("2026-02-03T04:05:06.789Z")
            .unwrap()
            .with_timezone(&Utc),
        CwasmArtifact::new(
            "artifact/github.com/example/repo/heads/main/abc123/runner.cwasm",
            "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
            123456,
            "43.0.1",
        ),
        "MyStrategy",
    )
}

fn logs_post() -> PresignedPost {
    PresignedPost {
        url: "https://run-logs.s3.us-east-1.amazonaws.com/".to_string(),
        fields: BTreeMap::from([(
            "key".to_string(),
            "logs/demo-algorithm/20260203T040506.789Z.log".to_string(),
        )]),
    }
}

fn request() -> RunRequest {
    RunRequest::from_record(
        &record(),
        "https://bucket.s3.example/runner.cwasm?X-Amz-Signature=signed".to_string(),
        "https://orders.example/requests".to_string(),
        logs_post(),
    )
}

#[test]
fn serialises_the_runner_contract() {
    let json = request().to_json().unwrap();
    let request: platform_runner::command::RunRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(
        request.cwasm_get_url,
        "https://bucket.s3.example/runner.cwasm?X-Amz-Signature=signed"
    );
    assert_eq!(
        request.cwasm_sha256,
        "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"
    );
    assert_eq!(request.cwasm_size, 123456);
    assert_eq!(request.wasmtime_version, "43.0.1");
    assert_eq!(request.order_request_url, "https://orders.example/requests");
    assert_eq!(request.owner, "demo-owner");
    match request.command {
        platform_runner::command::Command::Algorithm { strategy_name } => {
            assert_eq!(strategy_name, "MyStrategy")
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn serialises_camel_case_fields() {
    let json = request().to_json().unwrap();
    assert!(json.contains("\"cwasmGetUrl\":"));
    assert!(json.contains("\"cwasmSha256\":"));
    assert!(json.contains("\"cwasmSize\":123456"));
    assert!(json.contains("\"wasmtimeVersion\":"));
    assert!(json.contains("\"orderRequestUrl\":\"https://orders.example/requests\""));
    assert!(json.contains("\"owner\":\"demo-owner\""));
    assert!(json.contains("\"command\":\"algorithm\""));
    assert!(json.contains("\"strategyName\":\"MyStrategy\""));
    assert!(json.contains("\"logsPost\":"));
    assert!(json.contains("\"url\":\"https://run-logs.s3.us-east-1.amazonaws.com/\""));
}

#[test]
fn copies_the_record_details() {
    let request = request();
    let record = record();
    assert_eq!(request.cwasm_sha256, record.cwasm_sha256);
    assert_eq!(request.cwasm_size, record.cwasm_size);
    assert_eq!(request.wasmtime_version, record.wasmtime_version);
    assert_eq!(request.owner, record.owner);
    assert_eq!(request.logs_post, logs_post());
    match request.command {
        RunCommand::Algorithm { strategy_name } => {
            assert_eq!(strategy_name, record.strategy_name)
        }
    }
}

#[test]
fn includes_the_timestamp_in_the_log_key() {
    assert_eq!(
        RunRequest::log_key(&record()),
        "logs/demo-algorithm/20260203T040506.789Z.log"
    );
}
