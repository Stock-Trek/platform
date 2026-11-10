use crate::{OrderRequestDelivery, OrderRequestService, SignedOrderSubmission};
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use platform_errors::{BpError, BpResult};
use platform_messages::order_request::UnsignedOrderRequest;
use std::{convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;

pub struct OrderRequestServer {
    listener: TcpListener,
    service: Arc<OrderRequestService>,
    delivery: Arc<OrderRequestDelivery>,
}

impl OrderRequestServer {
    pub async fn bind(
        address: SocketAddr,
        service: Arc<OrderRequestService>,
        delivery: Arc<OrderRequestDelivery>,
    ) -> BpResult<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(BpError::external)?;
        Ok(Self {
            listener,
            service,
            delivery,
        })
    }

    pub async fn serve(self) -> BpResult<()> {
        loop {
            let (stream, _) = self.listener.accept().await.map_err(BpError::external)?;
            let service = self.service.clone();
            let delivery = self.delivery.clone();
            tokio::spawn(async move {
                let connection = http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| {
                            Self::respond(service.clone(), delivery.clone(), request)
                        }),
                    )
                    .await;
                if let Err(error) = connection {
                    tracing::warn!("order request connection failed: {error}");
                }
            });
        }
    }

    pub fn local_addr(&self) -> BpResult<SocketAddr> {
        self.listener.local_addr().map_err(BpError::external)
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
                    .unwrap_or_else(|_| Self::internal_error()),
                Err(error) => {
                    tracing::error!("failed to serialize the signed order request: {error}");
                    Self::internal_error()
                }
            },
            Err((status, message)) => {
                tracing::warn!("rejected order request: {message}");
                Response::builder()
                    .status(status)
                    .body(Full::new(Bytes::from(message)))
                    .unwrap_or_else(|_| Self::internal_error())
            }
        };
        Ok(response)
    }

    async fn dispatch(
        service: &OrderRequestService,
        delivery: &OrderRequestDelivery,
        request: Request<Incoming>,
    ) -> Result<SignedOrderSubmission, (StatusCode, String)> {
        if request.method() != Method::POST {
            return Err((
                StatusCode::METHOD_NOT_ALLOWED,
                "POST is required".to_string(),
            ));
        }
        let body = request
            .into_body()
            .collect()
            .await
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
            .to_bytes();
        if body.len() > MAX_REQUEST_BYTES {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "order request is too large".to_string(),
            ));
        }
        let unsigned: UnsignedOrderRequest = serde_json::from_slice(&body)
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        let submission = service
            .sign(&unsigned)
            .await
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        delivery
            .deliver(&submission)
            .await
            .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;
        Ok(submission)
    }

    fn internal_error() -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::from_static(b"internal error")))
            .expect("static response is valid")
    }
}
