use super::order_request_server_fixture::{Fixture, OWNER};
use crate::{OrderRequestDelivery, OrderRequestServer, OrderRequestService};
use chrono::Utc;
use platform_messages::order_request::UnsignedOrderRequest;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn signs_received_requests_and_delivers_them() {
    let (delivery_url, downstream) = Fixture::downstream().await;
    let (auth_service, token) = Fixture::auth_service().await;
    let service = Arc::new(OrderRequestService::new(auth_service.clone()));
    let delivery = Arc::new(OrderRequestDelivery::new(&delivery_url).unwrap());
    let server = OrderRequestServer::bind(Fixture::address(), service, delivery)
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let running = tokio::spawn(server.serve());
    let request = Fixture::unsigned();
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
    assert_eq!(body["cex"], "binance");
    assert_eq!(body["account"], "account-1");
    assert_eq!(body["owner"], OWNER);
    assert_eq!(body["tokenId"], token.id());
    let canonical: UnsignedOrderRequest = serde_json::from_value(body.clone()).unwrap();
    assert!(
        auth_service
            .verify_order_request(
                token.id(),
                &canonical.canonical_bytes().unwrap(),
                body["signature"].as_str().unwrap(),
                Utc::now(),
            )
            .await
            .unwrap()
            .is_valid()
    );
    running.abort();
}

#[tokio::test]
async fn rejects_a_request_without_a_jwt() {
    let (delivery_url, _) = Fixture::downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let service = Arc::new(OrderRequestService::new(auth_service));
    let delivery = Arc::new(OrderRequestDelivery::new(&delivery_url).unwrap());
    let server = OrderRequestServer::bind(Fixture::address(), service, delivery)
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let running = tokio::spawn(server.serve());

    let response = reqwest::Client::new()
        .post(format!("http://{address}/order-requests"))
        .json(&Fixture::unsigned())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    running.abort();
}

#[tokio::test]
async fn rejects_a_request_with_an_invalid_jwt() {
    let (delivery_url, _) = Fixture::downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let service = Arc::new(OrderRequestService::new(auth_service));
    let delivery = Arc::new(OrderRequestDelivery::new(&delivery_url).unwrap());
    let server = OrderRequestServer::bind(Fixture::address(), service, delivery)
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let running = tokio::spawn(server.serve());

    let response = reqwest::Client::new()
        .post(format!("http://{address}/order-requests"))
        .bearer_auth("not-a-jwt")
        .json(&Fixture::unsigned())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    running.abort();
}

#[tokio::test]
async fn rejects_invalid_json_with_a_stable_body() {
    let (delivery_url, _) = Fixture::downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let jwt = auth_service
        .issue_jwt_for_owner(OWNER, Utc::now())
        .await
        .unwrap();
    let (address, running) = Fixture::running_server(&delivery_url, auth_service).await;

    let response = reqwest::Client::new()
        .post(format!("http://{address}/order-requests"))
        .bearer_auth(jwt.token())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(r#"{"side":"buy""#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body,
        json!({
            "error": "invalid_request",
            "message": "the request is invalid"
        })
    );
    running.abort();
}

#[tokio::test]
async fn rejects_unsupported_methods_with_a_stable_body() {
    let (delivery_url, _) = Fixture::downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let (address, running) = Fixture::running_server(&delivery_url, auth_service).await;

    let response = reqwest::Client::new()
        .get(format!("http://{address}/order-requests"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body,
        json!({
            "error": "method_not_allowed",
            "message": "POST is required"
        })
    );
    running.abort();
}
