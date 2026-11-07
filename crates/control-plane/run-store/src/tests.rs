use crate::run_record::{
    ALGORITHM_ID, COMPLETED_AT, CWASM_KEY, CWASM_SHA256, CWASM_SIZE, ERROR, OWNER, SCHEDULED_AT,
    STATUS, STRATEGY_NAME, WASMTIME_VERSION,
};
use crate::run_status::RunStatus;
use crate::{CwasmArtifact, RunRecord};
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Utc};

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

#[test]
fn round_trips_a_record_through_item_attributes() {
    let record = record();
    let item = record.to_item();
    assert_eq!(RunRecord::from_item(&item).unwrap(), record);
}

#[test]
fn writes_timestamps_as_rfc3339_strings() {
    let item = record().to_item();
    assert_eq!(
        item.get(SCHEDULED_AT).unwrap().as_s().unwrap(),
        "2026-02-03T04:05:06.789Z"
    );
}

#[test]
fn writes_the_primary_key_attributes() {
    let item = record().to_item();
    assert_eq!(
        item.get(ALGORITHM_ID).unwrap().as_s().unwrap(),
        "demo-algorithm"
    );
    assert_eq!(item.get(OWNER).unwrap().as_s().unwrap(), "demo-owner");
    assert_eq!(item.get(STATUS).unwrap().as_s().unwrap(), "pending");
    assert!(!item.contains_key(COMPLETED_AT));
    assert!(!item.contains_key(ERROR));
}

#[test]
fn writes_failure_details() {
    let mut record = record();
    record.status = RunStatus::Failed;
    record.completed_at = Some(
        DateTime::parse_from_rfc3339("2026-02-03T04:06:07Z")
            .unwrap()
            .with_timezone(&Utc),
    );
    record.error = Some("runner exited with code 1".to_string());
    let item = record.to_item();
    assert_eq!(RunRecord::from_item(&item).unwrap(), record);
}

#[test]
fn rejects_malformed_items() {
    let record = record();
    for (name, value) in [
        (ALGORITHM_ID, AttributeValue::Null(true)),
        (OWNER, AttributeValue::Null(true)),
        (SCHEDULED_AT, AttributeValue::S("not-a-timestamp".into())),
        (CWASM_KEY, AttributeValue::S(String::new())),
        (CWASM_SHA256, AttributeValue::N("1".into())),
        (CWASM_SIZE, AttributeValue::N("-1".into())),
        (WASMTIME_VERSION, AttributeValue::Bool(true)),
        (STRATEGY_NAME, AttributeValue::Null(true)),
        (STATUS, AttributeValue::S("unknown".into())),
    ] {
        let mut item = record.to_item();
        item.insert(name.to_string(), value);
        assert!(
            RunRecord::from_item(&item).is_err(),
            "expected {name} to be rejected"
        );
    }
}

#[test]
fn rejects_items_with_missing_attributes() {
    let record = record();
    for name in [
        ALGORITHM_ID,
        OWNER,
        SCHEDULED_AT,
        CWASM_KEY,
        CWASM_SHA256,
        CWASM_SIZE,
        WASMTIME_VERSION,
        STRATEGY_NAME,
        STATUS,
    ] {
        let mut item = record.to_item();
        item.remove(name);
        assert!(
            RunRecord::from_item(&item).is_err(),
            "expected {name} to be required"
        );
    }
}

#[test]
fn parses_run_statuses() {
    for (value, expected) in [
        ("pending", RunStatus::Pending),
        ("running", RunStatus::Running),
        ("completed", RunStatus::Completed),
        ("failed", RunStatus::Failed),
    ] {
        assert_eq!(RunStatus::parse(value), Some(expected));
        assert_eq!(expected.as_str(), value);
        assert_eq!(expected.to_string(), value);
    }
    assert_eq!(RunStatus::parse("Pending"), None);
    assert_eq!(RunStatus::parse(""), None);
}

#[test]
fn formats_timestamps_for_lexicographic_sorting() {
    let earlier = record().scheduled_at;
    let later = earlier + chrono::Duration::milliseconds(1);
    assert!(RunRecord::format_timestamp(earlier) < RunRecord::format_timestamp(later));
    assert_eq!(
        RunRecord::parse_timestamp(&RunRecord::format_timestamp(earlier)).unwrap(),
        earlier
    );
}
