use super::Connector;
use crate::{
    clients::client::HttpClient,
    rate_limit::rate_limiters::RateLimitGuard,
    retry_after::RetryAfter,
    submission::{Submission, SubmissionOutcome},
};
use platform_errors::{BpError, BpResult, ConnectorError, RateLimitError, SendFailure};
use platform_exchange_types::{
    exchange::ETExchange, http::HttpResponse, new_types::UsageCount,
    rate_limited::RateLimitRestriction, request::ETHttpRequest, response::ETHttpResponse,
    server_time::ServerTimeResponse,
};
use std::time::Instant;
use tracing::{debug, warn};

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: HttpClient,
{
    pub async fn sync_clock_http(&self) -> BpResult<()> {
        debug!(exchange = self.exchange.name(), "syncing clock over HTTP");
        let server_time_request = self.exchange.server_time_request_http();
        let costs = self.validate_rate_limits(&server_time_request)?;
        let http_request = match server_time_request.try_into_http(&self.signer) {
            Ok(http_request) => http_request,
            Err(error) => {
                self.refund(costs)?;
                return Err(BpError::external(error));
            }
        };
        let start = Instant::now();
        let http_response = match self.client.send(http_request, self.request_timeout).await {
            Ok(http_response) => http_response,
            Err(error) => return self.on_send_failure(error, costs),
        };
        let round_trip_time = start.elapsed();
        let response =
            self.handle_http_response::<Exchange::ServerTimeResponseHttp>(http_response)?;
        let server_time = response
            .server_time()
            .map_err(|_| BpError::Connector(ConnectorError::MissingServerTime))?;
        self.clock.sync(server_time, round_trip_time)?;
        debug!(
            exchange = self.exchange.name(),
            round_trip_ms = round_trip_time.as_millis() as u64,
            "clock synced over HTTP"
        );
        Ok(())
    }
    pub fn submit_http<'connector, Request>(
        &'connector self,
        request: Request,
    ) -> BpResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send + 'connector,
        Exchange: Sync,
        Client: Sync,
    {
        let future = async move {
            let costs = self
                .validate_rate_limits(&request)
                .map_err(BpError::send_not_sent)?;
            self.send_http(request, costs).await
        };
        Ok(Submission::new(future))
    }
    async fn send_http<Request>(
        &self,
        mut request: Request,
        mut costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> BpResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send,
        Exchange: Sync,
        Client: Sync,
    {
        debug!(exchange = self.exchange.name(), "submitting HTTP request");
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
                    return self.submission_error_http(
                        request,
                        BpError::send_not_sent(error),
                        costs,
                    );
                }
            };
            request.set_timestamp(timestamp);
            let http_request = match request.clone().try_into_http(&self.signer) {
                Ok(http_request) => http_request,
                Err(error) => {
                    return self.submission_error_http(
                        request,
                        BpError::send_not_sent_external(error),
                        costs,
                    );
                }
            };
            let result = {
                let guard = RateLimitGuard::new(&self.rate_limiters, costs.clone());
                let result = self.client.send(http_request, self.request_timeout).await;
                guard.disarm();
                result
            };
            let error = match result {
                Ok(http_response) => match self.handle_http_response(http_response) {
                    Ok(response) => return Ok(SubmissionOutcome::Submitted(response)),
                    Err(error) => error,
                },
                Err(error) => error,
            };
            if retries_remaining == 0 || !error.retryability().is_retryable() {
                return self.submission_error_http(request, error, costs);
            }
            retries_remaining -= 1;
            warn!(
                exchange = self.exchange.name(),
                retries_remaining,
                error = %error,
                "HTTP request failed, retrying"
            );
            self.refund_if_not_sent(&error, &mut costs)?;
            match self.validate_rate_limits(&request) {
                Ok(retry_costs) => costs = retry_costs,
                Err(_) => return self.submission_error_http(request, error, costs),
            }
        }
    }
    fn handle_http_response<Response>(&self, http_response: HttpResponse) -> BpResult<Response>
    where
        Response: ETHttpResponse,
    {
        let status = http_response.status;
        let retry_after = RetryAfter::from_headers(&http_response.headers);
        if status == 429 {
            if let Ok(response) = Response::try_from_http(http_response) {
                self.set_rate_limits(&response)
                    .map_err(BpError::send_failed)?;
            }
            if let Some(retry_after) = retry_after {
                self.rate_limiters
                    .set_retry_after(retry_after)
                    .map_err(BpError::send_failed)?;
            }
            warn!(
                exchange = self.exchange.name(),
                status, "exchange rate limited the request"
            );
            return Err(BpError::send_failed(BpError::RateLimit(
                RateLimitError::RateLimited,
            )));
        }
        if !(200..300).contains(&status) {
            warn!(
                exchange = self.exchange.name(),
                status, "HTTP request failed"
            );
            if let Some(retry_after) = retry_after {
                self.rate_limiters
                    .set_retry_after(retry_after)
                    .map_err(BpError::send_unknown)?;
                // The exchange responded but is throttling us. The request reached
                // the exchange, so it cannot be reported as not sent.
                return Err(BpError::send_unknown(BpError::RateLimit(
                    RateLimitError::RateLimited,
                )));
            }
            let error = BpError::Connector(ConnectorError::GatewayHttpStatus { status });
            return Err(if status >= 500 {
                BpError::send_unknown(error)
            } else {
                BpError::send_failed(error)
            });
        }
        if let Some(retry_after) = retry_after {
            self.rate_limiters
                .set_retry_after(retry_after)
                .map_err(BpError::send_unknown)?;
        }
        let response = Response::try_from_http(http_response).map_err(|source| {
            BpError::send_unknown(BpError::Connector(ConnectorError::HttpParseError {
                source: Box::new(source),
            }))
        })?;
        self.set_rate_limits(&response)
            .map_err(BpError::send_unknown)?;
        Ok(response)
    }
    fn submission_error_http<Request>(
        &self,
        request: Request,
        error: BpError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> BpResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest,
    {
        if error.send_failure().is_some_and(SendFailure::is_unknown) {
            warn!(
                exchange = self.exchange.name(),
                error = %error,
                "request outcome unknown, verification required"
            );
            let verify = request.verification_request_http();
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
