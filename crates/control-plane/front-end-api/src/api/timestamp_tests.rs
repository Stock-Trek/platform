use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Sample {
    #[serde(with = "crate::api::timestamp::milliseconds")]
    at: DateTime<Utc>,
    #[serde(with = "crate::api::timestamp::milliseconds::option")]
    maybe: Option<DateTime<Utc>>,
}

fn sample(at: DateTime<Utc>, maybe: Option<DateTime<Utc>>) -> Sample {
    Sample { at, maybe }
}

#[test]
fn timestamps_use_millisecond_precision() {
    let at = Utc.with_ymd_and_hms(2026, 7, 14, 9, 30, 0).unwrap();
    let encoded = serde_json::to_value(sample(at, None)).expect("encode");
    assert_eq!(encoded["at"], "2026-07-14T09:30:00.000Z");
    assert!(encoded["maybe"].is_null());
}

#[test]
fn fractional_timestamps_are_truncated_to_milliseconds() {
    let at = Utc.with_ymd_and_hms(2026, 7, 14, 9, 30, 0).unwrap()
        + chrono::Duration::nanoseconds(456_789_012);
    let encoded = serde_json::to_value(sample(at, Some(at))).expect("encode");
    assert_eq!(encoded["at"], "2026-07-14T09:30:00.456Z");
    assert_eq!(encoded["maybe"], "2026-07-14T09:30:00.456Z");
}

#[test]
fn timestamps_round_trip() {
    let at =
        Utc.with_ymd_and_hms(2026, 7, 14, 9, 30, 0).unwrap() + chrono::Duration::milliseconds(41);
    let encoded = serde_json::to_string(&sample(at, Some(at))).expect("encode");
    let decoded: Sample = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(decoded, sample(at, Some(at)));
}

#[test]
fn deserialization_rejects_non_timestamps() {
    let raw = r#"{ "at": "soon", "maybe": null }"#;
    assert!(serde_json::from_str::<Sample>(raw).is_err());
}
