use super::order_request_server_fixture_tests::{Fixture, OWNER};
use crate::{OrderRequestDelivery, OrderRequestServer, OrderRequestService};
use chrono::Utc;
use platform_errors::BuildResult;
use std::sync::Arc;
use tokio::sync::oneshot;

#[tokio::test]
async fn drains_in_flight_requests_before_shutdown_completes() {
    let (delivery_url, downstream, release, received) = Fixture::controlled_downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let service = Arc::new(OrderRequestService::new(auth_service.clone()));
    let delivery = Arc::new(OrderRequestDelivery::new(&delivery_url).unwrap());
    let server = OrderRequestServer::bind(Fixture::address(), service, delivery)
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<BuildResult<()>>();
    let running = tokio::spawn(
        server.serve_with_shutdown(async move { shutdown_rx.await.unwrap_or(Ok(())) }),
    );
    let jwt = auth_service
        .issue_jwt_for_owner(OWNER, Utc::now())
        .await
        .unwrap();
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://{address}/order-requests"))
            .bearer_auth(jwt.token())
            .json(&Fixture::unsigned())
            .send()
            .await
            .unwrap()
    });

    received.await.unwrap();
    shutdown_tx.send(Ok(())).unwrap();
    release.send(()).unwrap();

    assert!(request.await.unwrap().status().is_success());
    downstream.await.unwrap();
    running.await.unwrap().unwrap();
}
