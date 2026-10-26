use crate::{
    context::{Converters, RsWebhookContext},
    event_response::EventResponse,
    responses::Responses,
};
use platform_errors::{BpError, BpResult, GitHubError, HttpError, SecretsError, SqsError};
use platform_messages::sqs_message::SqsMessage;
use platform_metrics::{event::EventMetrics, route::RouteMetrics};
use platform_time::{deadline::Deadline, request_context::RequestContext};
use http::{Response, StatusCode};
use lambda_http::{Body, Request, RequestExt};

const WEBHOOK_ROUTE: &str = "webhook";
const ERROR_TYPE_HTTP: &str = "http";

struct WebhookOutcome {
    response: Response<Body>,
    error_type: Option<&'static str>,
}

impl WebhookOutcome {
    fn success(response: BpResult<Response<Body>>) -> BpResult<Self> {
        Ok(Self {
            response: response?,
            error_type: None,
        })
    }

    fn failure(error_type: &'static str, response: BpResult<Response<Body>>) -> BpResult<Self> {
        Ok(Self {
            response: response?,
            error_type: Some(error_type),
        })
    }
}

pub struct WebhookHandler;

impl WebhookHandler {
    pub async fn handle(
        &self,
        context: &RsWebhookContext,
        event: Request,
    ) -> BpResult<Response<Body>> {
        let metrics = RouteMetrics::start(WEBHOOK_ROUTE);
        let result = self.handle_request(context, event).await;
        match result {
            Ok(WebhookOutcome {
                response,
                error_type,
            }) => {
                if response.status().is_success() {
                    metrics.record_success_status(response.status().as_u16());
                } else {
                    metrics.record_error_status(
                        error_type.unwrap_or(ERROR_TYPE_HTTP),
                        response.status().as_u16(),
                    );
                }
                Ok(response)
            }
            Err(BpError::Secrets(SecretsError::Unavailable { retry_after })) => {
                let status = StatusCode::SERVICE_UNAVAILABLE;
                metrics.record_error_status("secrets_unavailable", status.as_u16());
                Responses::service_unavailable(retry_after)
            }
            Err(error) if !error.retryability().is_retryable() && error.is_client_error() => {
                tracing::error!("rejecting permanently failing webhook request: {error}");
                let status = StatusCode::BAD_REQUEST;
                metrics.record_error_status(error.error_type(), status.as_u16());
                Responses::bad_request()
            }
            Err(error) if !error.retryability().is_retryable() => {
                tracing::error!("webhook request failed with a permanent server error: {error}");
                let status = StatusCode::INTERNAL_SERVER_ERROR;
                metrics.record_error_status(error.error_type(), status.as_u16());
                Responses::internal_server_error()
            }
            Err(error) => {
                metrics.record_error_status(
                    error.error_type(),
                    StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                );
                Err(error)
            }
        }
    }

    async fn handle_request(
        &self,
        context: &RsWebhookContext,
        event: Request,
    ) -> BpResult<WebhookOutcome> {
        let request_context = RequestContext {
            context,
            deadline: Deadline::new(event.lambda_context().deadline(), context.deadline_buffer()),
        };
        self.handle_event(request_context, event).await
    }

    async fn handle_event(
        &self,
        request_context: RequestContext<&RsWebhookContext>,
        event: Request,
    ) -> BpResult<WebhookOutcome> {
        let converters = request_context
            .context
            .converters(request_context.deadline)
            .await?;
        let response = self.convert(&request_context, &converters, &event).await?;
        if response != EventResponse::Unauthorized {
            return Self::respond(response);
        }
        tracing::warn!("webhook signature rejected; attempting secret refresh");
        match request_context
            .context
            .refresh_converters(request_context.deadline)
            .await
        {
            Ok(Some(converters)) => {
                let response = self.convert(&request_context, &converters, &event).await?;
                Self::respond(response)
            }
            Ok(None) => {
                tracing::debug!("secret refresh throttled; rejecting webhook without retry");
                WebhookOutcome::failure("refresh_throttled", Responses::unauthorized())
            }
            Err(error) => {
                tracing::warn!("failed to refresh secrets after signature rejection: {error}");
                Err(error)
            }
        }
    }

    /// Reduces the response of every converter into a single outcome, sending
    /// accepted messages to SQS as they are produced.
    async fn convert(
        &self,
        request_context: &RequestContext<&RsWebhookContext>,
        converters: &Converters,
        event: &Request,
    ) -> BpResult<EventResponse> {
        let mut ignored = false;
        let mut unauthorized = false;
        for converter in converters.iter() {
            match converter.event_response(event) {
                Ok(EventResponse::Accept(messages)) => {
                    let delivery_id = messages
                        .first()
                        .and_then(|message| message.source.delivery_id());
                    if let Some(delivery_id) = delivery_id
                        && request_context.context.dedup().is_duplicate(delivery_id)
                    {
                        tracing::info!(delivery_id, "ignoring replayed webhook delivery");
                        EventMetrics::record_duplicate_event();
                        return Ok(EventResponse::Duplicate);
                    }
                    let send_result = request_context
                        .context
                        .aws
                        .sqs
                        .send_batch(&messages, request_context.deadline)
                        .await;
                    match send_result {
                        Ok(()) => return Ok(EventResponse::Accept(messages)),
                        Err(error) if error.any_sent() => {
                            let Some((unsent_count, unsent)) =
                                Self::unsent_failure_messages(&error)
                            else {
                                if let Some(delivery_id) = delivery_id {
                                    request_context.context.dedup().forget(delivery_id);
                                }
                                return Err(error);
                            };
                            if unsent.is_empty() || unsent.len() != unsent_count {
                                if let Some(delivery_id) = delivery_id {
                                    request_context.context.dedup().forget(delivery_id);
                                }
                                tracing::error!(
                                    "SQS partially sent a delivery but did not identify every unsent message: {error}"
                                );
                                return Err(error);
                            }
                            tracing::warn!(
                                "SQS queued part of the delivery; resending {} unsent message(s) individually: {error}",
                                unsent.len()
                            );
                            match request_context
                                .context
                                .aws
                                .sqs
                                .send_individually(&unsent, request_context.deadline)
                                .await
                            {
                                Ok(()) => return Ok(EventResponse::Accept(messages)),
                                Err(error) => {
                                    if let Some(delivery_id) = delivery_id {
                                        request_context.context.dedup().forget(delivery_id);
                                    }
                                    tracing::error!(
                                        "SQS could not deliver {} unsent message(s) from a partial batch: {error}",
                                        unsent.len()
                                    );
                                    return Err(error);
                                }
                            }
                        }
                        Err(error) if !error.retryability().is_retryable() => {
                            if let Some(delivery_id) = delivery_id {
                                request_context.context.dedup().forget(delivery_id);
                            }
                            tracing::warn!("rejecting message that cannot be sent: {error}");
                            return Ok(EventResponse::Rejected);
                        }
                        Err(error) => {
                            if let Some(delivery_id) = delivery_id {
                                request_context.context.dedup().forget(delivery_id);
                            }
                            return Err(error);
                        }
                    }
                }
                Ok(EventResponse::Pong) => return Ok(EventResponse::Pong),
                Ok(EventResponse::Duplicate) => return Ok(EventResponse::Duplicate),
                Ok(EventResponse::Ignore) => ignored = true,
                Ok(EventResponse::Unauthorized) => unauthorized = true,
                Ok(EventResponse::WrongGitProvider) => continue,
                Ok(EventResponse::Rejected) => return Ok(EventResponse::Rejected),
                Err(BpError::Http(HttpError::Serde(error))) => {
                    tracing::warn!("rejecting malformed payload: {error}");
                    return Ok(EventResponse::Rejected);
                }
                Err(BpError::GitHub(GitHubError::HostInvalid { host: url })) => {
                    tracing::warn!("rejecting payload with unparseable host URL: {url:?}");
                    return Ok(EventResponse::Rejected);
                }
                Err(BpError::GitHub(GitHubError::HostNotAllowed { host: url })) => {
                    tracing::warn!("rejecting payload from disallowed host: {url:?}");
                    return Ok(EventResponse::Rejected);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(if unauthorized {
            EventResponse::Unauthorized
        } else if ignored {
            EventResponse::Ignore
        } else {
            EventResponse::Rejected
        })
    }

    fn unsent_failure_messages(error: &BpError) -> Option<(usize, Vec<SqsMessage>)> {
        let BpError::Sqs(SqsError::PartialBatch {
            unsent, failures, ..
        }) = error
        else {
            return None;
        };
        Some((
            *unsent,
            failures
                .iter()
                .filter_map(|failure| failure.source.clone())
                .collect(),
        ))
    }

    fn respond(response: EventResponse) -> BpResult<WebhookOutcome> {
        match response {
            EventResponse::Accept(_) => WebhookOutcome::success(Responses::accepted()),
            EventResponse::Pong => WebhookOutcome::success(Responses::ok("pong")),
            EventResponse::Duplicate => WebhookOutcome::success(Responses::no_content()),
            EventResponse::Ignore => WebhookOutcome::success(Responses::no_content()),
            EventResponse::Unauthorized => {
                WebhookOutcome::failure("unauthorized", Responses::unauthorized())
            }
            EventResponse::WrongGitProvider => {
                WebhookOutcome::failure("wrong_git_provider", Responses::bad_request())
            }
            EventResponse::Rejected => {
                WebhookOutcome::failure("rejected", Responses::bad_request())
            }
        }
    }
}
