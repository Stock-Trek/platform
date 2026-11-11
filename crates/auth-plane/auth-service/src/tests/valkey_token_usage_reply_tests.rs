use crate::{
    TokenAuditAction, TokenAuditEvent, TokenUsage, valkey_token_usage_reply::ValkeyTokenUsageReply,
};
use chrono::Utc;
use redis::Value;

struct ReplyFixture;

impl ReplyFixture {
    fn payload(usage: &TokenUsage) -> Value {
        Value::Array(vec![
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(serde_json::to_vec(usage).unwrap()),
        ])
    }

    fn reply(entries: Vec<Value>) -> Value {
        Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"auth-usage:pending".to_vec()),
            Value::Array(entries),
        ])])
    }
}

#[test]
fn parses_stream_records_into_reservations() {
    let usage = TokenUsage::for_token(
        "token-a",
        Utc::now(),
        vec![TokenAuditEvent::new(
            TokenAuditAction::Authenticated,
            Utc::now(),
        )],
    );
    let record = Value::Array(vec![
        Value::BulkString(b"1-0".to_vec()),
        ReplyFixture::payload(&usage),
    ]);

    let reservations = ValkeyTokenUsageReply::parse(ReplyFixture::reply(vec![record])).unwrap();

    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].id(), "1-0");
    assert_eq!(reservations[0].usage().token_id(), Some("token-a"));
}

#[test]
fn treats_an_empty_read_as_no_reservations() {
    assert!(ValkeyTokenUsageReply::parse(Value::Nil).unwrap().is_empty());
}

#[test]
fn rejects_a_record_without_a_payload() {
    let record = Value::Array(vec![
        Value::BulkString(b"1-0".to_vec()),
        Value::Array(vec![Value::BulkString(b"other".to_vec())]),
    ]);

    assert!(ValkeyTokenUsageReply::parse(ReplyFixture::reply(vec![record])).is_err());
}
