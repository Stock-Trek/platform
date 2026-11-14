use crate::{OrderRequestDelivery, OrderRequestError, OrderRequestService, SignedOrderSubmission};
use chrono::Utc;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{AUTHORIZATION, CONTENT_LENGTH},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use platform_errors::PlatformError;
use platform_messages::UnsignedOrderRequest;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::watch, time::timeout};

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);

pub struct OrderRequestConnection;

impl OrderRequestConnection {
    pub async fn serve(
        stream: TcpStream,
        service: Arc<OrderRequestService>,
        delivery: Arc<OrderRequestDelivery>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut connection = Box::pin(
            http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |request| {
                        Self::respond(service.clone(), delivery.clone(), request)
                    }),
                ),
        );
        tokio::select! {
            result = connection.as_mut() => Self::log_connection(result),
            _ = Self::wait_for_shutdown(&mut shutdown) => {
                connection.as_mut().graceful_shutdown();
                Self::log_connection(connection.as_mut().await);
            }
        }
    }

    async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
        let _ = shutdown.wait_for(|shutdown| *shutdown).await;
    }

    fn log_connection(result: Result<(), hyper::Error>) {
        if let Err(error) = result {
            tracing::warn!("order request connection failed: {error}");
        }
    }

    async fn respond(
        service: Arc<OrderRequestService>,
        delivery: Arc<OrderRequestDelivery>,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let response = match Self::dispatch(&service, &delivery, request).await {
            Ok(submission) => match serde_json::to_vec(&submission) {
                Ok(body) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap_or_else(|_| OrderRequestError::Internal.response()),
                Err(error) => {
                    tracing::error!("failed to serialize the signed order request: {error}");
                    OrderRequestError::Internal.response()
                }
            },
            Err((error, detail)) => {
                tracing::warn!(%detail, "rejected order request");
                error.response()
            }
        };
        Ok(response)
    }

    async fn dispatch(
        service: &OrderRequestService,
        delivery: &OrderRequestDelivery,
        request: Request<Incoming>,
    ) -> Result<SignedOrderSubmission, (OrderRequestError, String)> {
        if request.method() != Method::POST {
            return Err((
                OrderRequestError::MethodNotAllowed,
                "POST is required".to_string(),
            ));
        }
        let jwt = Self::bearer(&request)?;
        Self::ensure_content_length(&request)?;
        let body = Self::read_body(request.into_body()).await?;
        let unsigned: UnsignedOrderRequest = serde_json::from_slice(&body).map_err(|error| {
            (
                OrderRequestError::InvalidRequest,
                format!("invalid order request: {error}"),
            )
        })?;
        let claims = service
            .authenticate(&jwt, Utc::now())
            .await
            .map_err(|error| {
                (
                    OrderRequestError::from_authentication_error(&error),
                    error.to_string(),
                )
            })?;
        let submission = service
            .sign_for_caller(&unsigned, &claims)
            .await
            .map_err(Self::sign_error)?;
        delivery
            .deliver(&submission)
            .await
            .map_err(|error| (OrderRequestError::ServiceUnavailable, error.to_string()))?;
        Ok(submission)
    }

    fn sign_error(error: PlatformError) -> (OrderRequestError, String) {
        let public = OrderRequestError::from_signing_error(&error);
        (public, error.to_string())
    }

    async fn read_body(body: Incoming) -> Result<Bytes, (OrderRequestError, String)> {
        let body = Limited::new(body, MAX_REQUEST_BYTES);
        let collected = timeout(BODY_READ_TIMEOUT, body.collect())
            .await
            .map_err(|_| {
                (
                    OrderRequestError::RequestTimeout,
                    "order request body timed out".to_string(),
                )
            })?
            .map_err(|error| {
                if error.downcast_ref::<LengthLimitError>().is_some() {
                    (
                        OrderRequestError::PayloadTooLarge,
                        "order request is too large".to_string(),
                    )
                } else {
                    (OrderRequestError::InvalidRequest, error.to_string())
                }
            })?;
        Ok(collected.to_bytes())
    }

    fn ensure_content_length(
        request: &Request<Incoming>,
    ) -> Result<(), (OrderRequestError, String)> {
        let too_large = request
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > MAX_REQUEST_BYTES);
        if too_large {
            Err((
                OrderRequestError::PayloadTooLarge,
                "order request is too large".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn bearer(request: &Request<Incoming>) -> Result<String, (OrderRequestError, String)> {
        let value = request.headers().get(AUTHORIZATION).ok_or_else(|| {
            (
                OrderRequestError::Unauthorized,
                "bearer JWT is required".to_string(),
            )
        })?;
        let value = value.to_str().map_err(|error| {
            (
                OrderRequestError::Unauthorized,
                format!("invalid authorization header: {error}"),
            )
        })?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| {
                (
                    OrderRequestError::Unauthorized,
                    "authorization must use the Bearer scheme".to_string(),
                )
            })?;
        Ok(token.to_string())
    }
}
