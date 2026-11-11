use super::order_request_server_fixture_tests::{Fixture, OWNER};
use crate::{OrderRequestDelivery, OrderRequestServer, OrderRequestService};
use chrono::Utc;
use std::sync::Arc;

#[tokio::test]
async fn signs_for_the_owner_authenticated_by_the_jwt() {
    let (delivery_url, downstream) = Fixture::downstream().await;
    let (auth_service, token) = Fixture::auth_service().await;
    let service = Arc::new(OrderRequestService::new(auth_service.clone()));
    let delivery = Arc::new(OrderRequestDelivery::new(&delivery_url).unwrap());
    let server = OrderRequestServer::bind(Fixture::address(), service, delivery)
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let running = tokio::spawn(server.serve());
    let request = Fixture::unsigned().with_owner("body-owner");
    let jwt = auth_service
        .issue_jwt_for_owner(OWNER, Utc::now())
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("http://{address}/order-requests"))
        .bearer_auth(jwt.token())
        .json(&request)
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body: serde_json::Value = serde_json::from_str(&downstream.await.unwrap()).unwrap();
    assert_eq!(body["owner"], OWNER);
    assert_eq!(body["tokenId"], token.id());
    running.abort();
}
