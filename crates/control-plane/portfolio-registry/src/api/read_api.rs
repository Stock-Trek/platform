use super::{ApiError, RequestHandler, bearer, empty_response, json_response};
use crate::RegistryService;
use async_trait::async_trait;
use chrono::Utc;
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
};
use platform_auth_service::JwksCache;
use std::sync::Arc;

pub struct ReadApi {
    service: Arc<RegistryService>,
    jwks: Arc<JwksCache>,
}

impl ReadApi {
    pub fn new(service: Arc<RegistryService>, jwks: JwksCache) -> Self {
        Self {
            service,
            jwks: Arc::new(jwks),
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
        if request.uri().path() != "/v1/accounts" {
            return Err(ApiError::not_found());
        }
        if request.method() != Method::GET {
            return Err(ApiError::method_not_allowed());
        }
        let jwt = bearer(&request)?;
        let claims = self
            .jwks
            .verify(jwt, Utc::now())
            .await
            .map_err(|error| ApiError::unauthorized(error.to_string()))?;
        let accounts = self
            .service
            .accounts(claims.owner())
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
        Ok(json_response(StatusCode::OK, &accounts))
    }
}

#[async_trait]
impl RequestHandler for ReadApi {
    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        match self.dispatch(request).await {
            Ok(response) => response,
            Err(error) => error.response(),
        }
    }
}
