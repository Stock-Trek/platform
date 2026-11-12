use super::order_request_server_fixture_tests::Fixture;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

#[tokio::test]
async fn rejects_a_body_larger_than_the_limit_before_delivery() {
    let (delivery_url, downstream) = Fixture::downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let (address, running) = Fixture::running_server(&delivery_url, auth_service).await;
    let body = vec![b' '; MAX_REQUEST_BYTES + 1];

    let response = reqwest::Client::new()
        .post(format!("http://{address}/order-requests"))
        .bearer_auth("not-a-jwt")
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert!(!downstream.is_finished());
    running.abort();
    downstream.abort();
}
