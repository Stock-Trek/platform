use super::Connector;
use crate::{
    clients::client::WebsocketClient,
    rate_limit::rate_limiters::RateLimitGuard,
    submission::{Submission, SubmissionOutcome},
};
use futures_timer::Delay;
use platform_errors::{BpError, BpResult, ConnectorError, SendFailure, TimeoutError};
use platform_exchange_types::{
    exchange::ETExchange, new_types::UsageCount, rate_limited::RateLimitRestriction,
    request::ETWebsocketRequest, response::ETWebsocketResponse, server_time::ServerTimeResponse,
    websocket_id::ETWebsocketId,
};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Instant,
};
use tracing::{debug, warn};

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> BpResult<()> {
        debug!(exchange = self.exchange.name(), "connecting websocket");
        self.client.connect().await
    }
    pub fn is_connected(&self) -> BpResult<bool> {
        Ok(self.client.is_connected())
    }
    pub async fn disconnect(&self) -> BpResult<()> {
        debug!(exchange = self.exchange.name(), "disconnecting websocket");
        self.client.disconnect().await
    }
    pub async fn sync_clock_websocket(&self) -> BpResult<()> {
        debug!(
            exchange = self.exchange.name(),
            "syncing clock over websocket"
        );
        let server_time_request = self.exchange.server_time_request_websocket();
        let costs = self.validate_rate_limits(&server_time_request)?;
        let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
        let (websocket_request, response_matcher) =
            match server_time_request.try_into_websocket(&self.signer, id) {
                Ok(request) => request,
                Err(error) => {
                    self.refund(costs)?;
                    return Err(BpError::external(error));
                }
            };
        let start = Instant::now();
        let response: Exchange::ServerTimeResponseWebsocket =
            match self.send_wait(websocket_request, response_matcher).await {
                Ok(response) => response,
                Err(error) => return self.on_send_failure(error, costs),
            };
        let round_trip_time = start.elapsed();
        let server_time = response
            .server_time()
            .map_err(|_| BpError::Connector(ConnectorError::MissingServerTime))?;
        self.clock.sync(server_time, round_trip_time)?;
        debug!(
            exchange = self.exchange.name(),
            round_trip_ms = round_trip_time.as_millis() as u64,
            "clock synced over websocket"
        );
        Ok(())
    }
    pub fn submit_websocket<'connector, Request>(
        &'connector self,
        request: Request,
    ) -> BpResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest<Exchange = Exchange> + Send + 'connector,
        Exchange: Sync,
    {
        let future = async move {
            let costs = self
                .validate_rate_limits(&request)
                .map_err(BpError::send_not_sent)?;
            self.send_websocket(request, costs).await
        };
        Ok(Submission::new(future))
    }
    async fn send_websocket<Request>(
        &self,
        mut request: Request,
        mut costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> BpResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest<Exchange = Exchange> + Send,
        Exchange: Sync,
    {
        debug!(
            exchange = self.exchange.name(),
            "submitting websocket request"
        );
        let is_idempotent = request.is_idempotent();
        let is_signed = request.is_signed();
        let mut retries_remaining = if is_idempotent { self.max_retries } else { 0 };
        loop {
            let timestamp = match if is_signed {
                self.clock.server_time_estimate()
            } else {
                self.clock.server_time_estimate_unchecked()
            } {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    return self.submission_error_websocket(
                        request,
                        BpError::send_not_sent(error),
                        costs,
                    );
                }
            };
            request.set_timestamp(timestamp);
            let websocket_id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
            let (websocket_request, response_matcher) = match request
                .clone()
                .try_into_websocket(&self.signer, websocket_id)
            {
                Ok(request) => request,
                Err(error) => {
                    return self.submission_error_websocket(
                        request,
                        BpError::send_not_sent_external(error),
                        costs,
                    );
                }
            };
            let result = {
                let guard = RateLimitGuard::new(&self.rate_limiters, costs.clone());
                let result = self.send_wait(websocket_request, response_matcher).await;
                guard.disarm();
                result
            };
            let error = match result {
                Ok(response) => return Ok(SubmissionOutcome::Submitted(response)),
                Err(error) => error,
            };
            if retries_remaining == 0 || !error.retryability().is_retryable() {
                return self.submission_error_websocket(request, error, costs);
            }
            retries_remaining -= 1;
            warn!(
                exchange = self.exchange.name(),
                retries_remaining,
                error = %error,
                "websocket request failed, retrying"
            );
            self.refund_if_not_sent(&error, &mut costs)?;
            match self.validate_rate_limits(&request) {
                Ok(retry_costs) => costs = retry_costs,
                Err(_) => return self.submission_error_websocket(request, error, costs),
            }
        }
    }
    async fn send_wait<Response>(
        &self,
        message: String,
        response_matcher: Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>,
    ) -> BpResult<Response>
    where
        Response: ETWebsocketResponse,
    {
        let listener = match self.websocket_listener.as_ref() {
            Some(listener) => listener,
            None => {
                return Err(BpError::send_not_sent(BpError::Connector(
                    ConnectorError::WebsocketListenerMissing,
                )));
            }
        };
        let waiter = match listener.waiter_for_filtered_response(response_matcher) {
            Ok(waiter) => waiter,
            Err(error) => {
                return Err(BpError::send_not_sent(error));
            }
        };
        let exchange = self.exchange.name();
        let start = Instant::now();
        debug!(exchange, "sending websocket request");
        self.client.send(message, self.request_timeout).await?;
        let remaining = self.request_timeout.saturating_sub(start.elapsed());
        let mut waiter = Box::pin(waiter);
        let mut delay = Box::pin(Delay::new(remaining));
        let response_value = poll_fn(move |cx| match waiter.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(BpError::send_unknown)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    warn!(exchange, "websocket request timed out");
                    Poll::Ready(Err(BpError::send_unknown(BpError::Timeout(
                        TimeoutError::TimedOut,
                    ))))
                }
                Poll::Pending => Poll::Pending,
            },
        })
        .await?;
        let response = Response::try_from_websocket(response_value).map_err(|source| {
            BpError::send_unknown(BpError::Connector(ConnectorError::WebsocketParseError {
                source: Box::new(source),
            }))
        })?;
        self.set_rate_limits(&response)
            .map_err(BpError::send_unknown)?;
        Ok(response)
    }
    fn submission_error_websocket<Request>(
        &self,
        request: Request,
        error: BpError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> BpResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest,
    {
        if error.send_failure().is_some_and(SendFailure::is_unknown) {
            warn!(
                exchange = self.exchange.name(),
                error = %error,
                "request outcome unknown, verification required"
            );
            let verify = request.verification_request_websocket();
            let retry = if request.is_idempotent() {
                Some(request)
            } else {
                None
            };
            Ok(SubmissionOutcome::Unknown { retry, verify })
        } else {
            self.on_send_failure(error, costs)
        }
    }
}
