use super::*;
use crate::{clients::client::HttpClient, clock::Clock, submission::SubmissionOutcome};
use platform_errors::{BpError, BpResult, ConnectorError, RateLimitError, SendFailure};
use platform_exchange_types::{
    binance::{
        exchange::BinanceExchange,
        klines::{BinanceKlinesInterval, BinanceKlinesRequest},
    },
    encode::ByteEncoder,
    exchange::ETExchange,
    http::{HttpRequest, HttpResponse},
    new_types::UsageCount,
    rate_limited::RateLimitRestriction,
    signer::Signer,
};
use std::{future::poll_fn, task::Poll, time::Duration};

struct UnknownOutcomeClient;

#[async_trait::async_trait]
impl HttpClient for UnknownOutcomeClient {
    async fn send(&self, _request: HttpRequest, _timeout: Duration) -> BpResult<HttpResponse> {
        Err(BpError::send_unknown(BpError::System(
            platform_errors::SystemError::PoisonedLock,
        )))
    }
}

struct PendingClient;

#[async_trait::async_trait]
impl HttpClient for PendingClient {
    async fn send(&self, _request: HttpRequest, _timeout: Duration) -> BpResult<HttpResponse> {
        std::future::pending().await
    }
}

fn connector_with<Client>(client: Client) -> Connector<BinanceExchange, Client> {
    let exchange = BinanceExchange;
    Connector {
        rate_limiters: Connector::rate_limiters(exchange.default_capacity())
            .expect("valid rate limiters"),
        exchange,
        clock: Clock::default(),
        signer: Signer::new_unencrypted(ByteEncoder::HexLower),
        client,
        request_timeout: Duration::from_secs(1),
        max_retries: 0,
        websocket_listener: None,
    }
}

fn connector() -> Connector<BinanceExchange, UnknownOutcomeClient> {
    connector_with(UnknownOutcomeClient)
}

fn klines_request() -> BinanceKlinesRequest {
    BinanceKlinesRequest::new("BTCUSDT".into(), BinanceKlinesInterval::ONE_MINUTE)
}

fn remaining_weight<Client>(connector: &Connector<BinanceExchange, Client>) -> UsageCount {
    connector
        .remaining_rate_limit_capacity()
        .expect("remaining capacity")
        .into_iter()
        .find_map(|(rate_limit, remaining)| {
            (rate_limit.restriction == RateLimitRestriction::Weight).then_some(remaining)
        })
        .expect("weight rate limit")
}

#[tokio::test]
async fn http_submission_acquires_rate_limits_only_when_awaited() {
    let connector = connector();
    let initial_weight = remaining_weight(&connector);

    let submission = connector
        .submit_http(klines_request())
        .expect("submission construction");
    assert_eq!(remaining_weight(&connector), initial_weight);

    drop(submission);
    assert_eq!(remaining_weight(&connector), initial_weight);

    let submission = connector
        .submit_http(klines_request())
        .expect("submission construction");
    assert!(matches!(
        submission.wait().await,
        Ok(SubmissionOutcome::Unknown { .. })
    ));
    assert_eq!(
        remaining_weight(&connector),
        initial_weight - UsageCount::TWO
    );
}

#[tokio::test]
async fn dropping_in_flight_http_submission_refunds_rate_limits() {
    let connector = connector_with(PendingClient);
    let initial_weight = remaining_weight(&connector);

    let mut submission = Box::pin(
        connector
            .submit_http(klines_request())
            .expect("submission construction")
            .wait(),
    );

    poll_fn(|cx| {
        assert!(submission.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;

    assert_eq!(
        remaining_weight(&connector),
        initial_weight - UsageCount::TWO
    );

    drop(submission);

    assert_eq!(remaining_weight(&connector), initial_weight);
}

#[tokio::test]
async fn http_rate_limit_rejection_is_deferred_until_awaited() {
    let connector = connector();
    connector
        .rate_limiters
        .set_retry_after(Duration::from_secs(60))
        .expect("set retry after");

    let submission = connector
        .submit_http(klines_request())
        .expect("submission construction must not acquire rate limits");

    let error = submission
        .wait()
        .await
        .expect_err("rate limited submission");
    assert_eq!(error.send_failure(), Some(SendFailure::NotSent));
    assert!(matches!(
        error,
        BpError::Connector(ConnectorError::Send { source, .. })
            if matches!(*source, BpError::RateLimit(RateLimitError::RateLimited))
    ));
}
#[test]
fn explicit_url_constructs_connector_without_environment_config() {
    let connector = Connector::try_new_http_reqwest(
        "https://api.binance.com/api/v3".to_string(),
        BinanceExchange,
        Signer::new_unencrypted(ByteEncoder::HexLower),
        Duration::from_secs(1),
        0,
    );

    assert!(connector.is_ok());
}

#[test]
fn explicit_url_is_validated() {
    let connector = Connector::try_new_http_reqwest(
        "not a url".to_string(),
        BinanceExchange,
        Signer::new_unencrypted(ByteEncoder::HexLower),
        Duration::from_secs(1),
        0,
    );

    assert!(connector.is_err());
}
