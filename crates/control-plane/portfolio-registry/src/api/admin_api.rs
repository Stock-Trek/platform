use super::{ApiError, RequestHandler, bearer, empty_response, json_response, read_json};
use crate::RegistryService;
use async_trait::async_trait;
use chrono::Utc;
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
};
use platform_errors::BpError;
use serde::Deserialize;
use std::sync::Arc;
use subtle::ConstantTimeEq;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OwnershipRequest {
    owner: String,
    cex: String,
    account_id: String,
}

pub struct AdminApi {
    service: Arc<RegistryService>,
    token: String,
}

impl AdminApi {
    pub fn new(service: Arc<RegistryService>, token: impl Into<String>) -> Self {
        Self {
            service,
            token: token.into(),
        }
    }

    async fn dispatch(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, ApiError> {
        if request.uri().path() == "/healthz" {
            return if request.method() == Method::GET {
                Ok(empty_response(StatusCode::NO_CONTENT))
            } else {
                Err(ApiError::method_not_allowed())
            };
        }
        if request.uri().path() != "/v1/ownership" {
            return Err(ApiError::not_found());
        }
        self.authorize(&request)?;
        match *request.method() {
            Method::POST => {
                let command: OwnershipRequest = read_json(request).await?;
                let mapping = self
                    .service
                    .add(
                        &command.owner,
                        &command.cex,
                        &command.account_id,
                        Utc::now(),
                    )
                    .await
                    .map_err(Self::service_error)?;
                Ok(json_response(StatusCode::OK, &mapping))
            }
            Method::DELETE => {
                let command: OwnershipRequest = read_json(request).await?;
                let mapping = self
                    .service
                    .remove(
                        &command.owner,
                        &command.cex,
                        &command.account_id,
                        Utc::now(),
                    )
                    .await
                    .map_err(Self::service_error)?;
                Ok(json_response(StatusCode::OK, &mapping))
            }
            _ => Err(ApiError::method_not_allowed()),
        }
    }

    fn authorize(&self, request: &Request<Incoming>) -> Result<(), ApiError> {
        let provided = bearer(request)?;
        let matches: bool = provided.as_bytes().ct_eq(self.token.as_bytes()).into();
        if matches {
            Ok(())
        } else {
            Err(ApiError::unauthorized("invalid admin token"))
        }
    }

    fn service_error(error: BpError) -> ApiError {
        ApiError::bad_request(error.to_string())
    }
}

#[async_trait]
impl RequestHandler for AdminApi {
    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        match self.dispatch(request).await {
            Ok(response) => response,
            Err(error) => error.response(),
        }
    }
}
