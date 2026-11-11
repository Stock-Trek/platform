use super::order_request_server_fixture::{Fixture, OWNER};
use crate::{
    AccountKey, OrderRequestDelivery, OrderRequestServer, OrderRequestService, PortfolioService,
};
use chrono::Utc;
use std::sync::Arc;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

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

#[tokio::test]
async fn rejects_an_account_not_owned_by_the_authenticated_owner() {
    let (delivery_url, _) = Fixture::downstream().await;
    let (auth_service, _) = Fixture::auth_service().await;
    let portfolio = Arc::new(PortfolioService::in_memory());
    let account = AccountKey::from((CexId::Binance, AccountId("account-2".to_string())));
    portfolio.track_user_account(OWNER, account).await.unwrap();
    let service =
        Arc::new(OrderRequestService::new(auth_service.clone()).with_portfolio_service(portfolio));
    let delivery = Arc::new(OrderRequestDelivery::new(&delivery_url).unwrap());
    let server = OrderRequestServer::bind(Fixture::address(), service, delivery)
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let running = tokio::spawn(server.serve());
    let jwt = auth_service
        .issue_jwt_for_owner(OWNER, Utc::now())
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("http://{address}/order-requests"))
        .bearer_auth(jwt.token())
        .json(&Fixture::unsigned())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    running.abort();
}
