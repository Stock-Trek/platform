use super::sqs_test_server_tests::{CapturedSqsRequest, MockSqs};
use crate::{SqsTokenAuditLog, TokenAuditAction, TokenAuditEvent, TokenAuditLog};
use aws_sdk_sqs::{
    Client as SqsClient,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
};
use chrono::Utc;
use platform_errors::BpError;
use platform_sqs::sqs::{Sqs, SqsConfig};
use platform_time::timeouts::Timeouts;
use std::time::Duration;

const QUEUE_URL: &str = "http://queue.test/audit";

struct SqsAuditFixture;

impl SqsAuditFixture {
    fn audit(server: &MockSqs, timeout: Duration) -> SqsTokenAuditLog {
        let config = aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(server.url())
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .build();
        let timeouts = Timeouts::from_durations(timeout, timeout);
        let sqs = Sqs::new(
            SqsClient::from_conf(config),
            timeouts,
            SqsConfig::new(QUEUE_URL.to_string()),
        );
        SqsTokenAuditLog::new(sqs, timeout)
    }

    fn event(token_id: &str) -> TokenAuditEvent {
        TokenAuditEvent::new(TokenAuditAction::Signed, Utc::now())
            .with_token(token_id, Some("owner-a"))
    }

    fn only_request(server: &MockSqs) -> CapturedSqsRequest {
        let mut requests = server.requests();
        assert_eq!(requests.len(), 1);
        requests.remove(0)
    }

    fn events(request: &CapturedSqsRequest) -> Vec<TokenAuditEvent> {
        let bodies: Vec<&str> = if request.target.ends_with(".SendMessageBatch") {
            request.body["Entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["MessageBody"].as_str().unwrap())
                .collect()
        } else {
            vec![request.body["MessageBody"].as_str().unwrap()]
        };
        bodies
            .into_iter()
            .map(|body| serde_json::from_str(body).unwrap())
            .collect()
    }
}

#[tokio::test]
async fn record_sends_one_event_to_sqs() {
    let server = MockSqs::start();
    let audit = SqsAuditFixture::audit(&server, Duration::from_secs(1));
    let event = SqsAuditFixture::event("token-a");

    audit.record(event.clone()).await.unwrap();

    let request = SqsAuditFixture::only_request(&server);
    assert!(
        request.target.ends_with(".SendMessageBatch"),
        "unexpected target: {}",
        request.target
    );
    assert_eq!(request.body["QueueUrl"], QUEUE_URL);
    assert_eq!(SqsAuditFixture::events(&request), vec![event]);
}

#[tokio::test]
async fn record_batch_sends_all_events_in_one_batch() {
    let server = MockSqs::start();
    let audit = SqsAuditFixture::audit(&server, Duration::from_secs(1));
    let events = [
        SqsAuditFixture::event("token-a"),
        SqsAuditFixture::event("token-b"),
    ];

    audit.record_batch(&events).await.unwrap();

    let request = SqsAuditFixture::only_request(&server);
    assert!(request.target.ends_with(".SendMessageBatch"));
    assert_eq!(request.body["QueueUrl"], QUEUE_URL);
    assert_eq!(SqsAuditFixture::events(&request), events);
}

#[tokio::test]
async fn record_batch_does_not_contact_sqs_for_no_events() {
    let server = MockSqs::start();
    let audit = SqsAuditFixture::audit(&server, Duration::from_secs(1));

    audit.record_batch(&[]).await.unwrap();

    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn record_propagates_sqs_delivery_failures() {
    let server = MockSqs::start();
    server.fail_requests();
    let audit = SqsAuditFixture::audit(&server, Duration::from_secs(2));

    let error = audit
        .record(SqsAuditFixture::event("token-a"))
        .await
        .unwrap_err();

    assert!(matches!(error, BpError::Sqs(_)));
}
