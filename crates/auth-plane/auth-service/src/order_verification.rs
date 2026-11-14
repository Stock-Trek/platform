use crate::{AuthService, OrderRequestSigner, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::PlatformResult;

pub const CLOCK_SKEW_SECONDS: i64 = 60;

impl AuthService {
    pub async fn verify_order_request(
        &self,
        token_id: &str,
        order_request: &[u8],
        signature: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<OrderRequestVerification> {
        let token = match self.signature_token(token_id, now).await {
            Ok(Some(token)) => token,
            Ok(None) => {
                return Ok(OrderRequestVerification::Invalid(
                    InvalidOrderRequest::UnknownToken,
                ));
            }
            Err(error) => {
                tracing::warn!(%error, "order request verification could not load the token");
                return Ok(OrderRequestVerification::Retryable);
            }
        };
        if Self::ensure_active(&token, now).is_err() {
            return self
                .reject_signature(
                    token_id,
                    Some(&token),
                    now,
                    InvalidOrderRequest::InactiveToken,
                )
                .await;
        }
        if Self::ensure_scope(&token, TokenScope::OrdersWrite).is_err() {
            return self
                .reject_signature(
                    token_id,
                    Some(&token),
                    now,
                    InvalidOrderRequest::MissingScope,
                )
                .await;
        }
        let (signed, context) = match self.validate_order_signature(token_id, signature, now) {
            Ok(validated) => validated,
            Err(reason) => {
                return self
                    .reject_signature(token_id, Some(&token), now, reason)
                    .await;
            }
        };
        let signer = match OrderRequestSigner::new(token.secret()) {
            Ok(signer) => signer,
            Err(error) => {
                return self
                    .reject_retryable(token_id, Some(&token), now, &error)
                    .await;
            }
        };
        match signer.verify_order_request(&context, order_request, signed.signature()) {
            Ok(true) => {}
            Ok(false) => {
                return self
                    .reject_signature(
                        token_id,
                        Some(&token),
                        now,
                        InvalidOrderRequest::SignatureMismatch,
                    )
                    .await;
            }
            Err(error) => {
                return self
                    .reject_retryable(token_id, Some(&token), now, &error)
                    .await;
            }
        }
        let reservation = match self.reserve_order_request_replay(&context, now).await {
            Ok(Some(reservation)) => reservation,
            Ok(None) => {
                return self
                    .reject_signature(token_id, Some(&token), now, InvalidOrderRequest::Replay)
                    .await;
            }
            Err(error) => {
                return self
                    .reject_retryable(token_id, Some(&token), now, &error)
                    .await;
            }
        };
        match self.commit_signature_use(token, now, &reservation).await {
            Ok(verification) => Ok(verification),
            Err(error) => self.reject_retryable(token_id, None, now, &error).await,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderRequestVerification {
    Valid,
    Invalid(InvalidOrderRequest),
    Retryable,
}

impl OrderRequestVerification {
    pub fn is_valid(self) -> bool {
        matches!(self, Self::Valid)
    }

    pub fn is_invalid(self) -> bool {
        matches!(self, Self::Invalid(_))
    }

    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Retryable)
    }

    pub fn invalid_reason(self) -> Option<InvalidOrderRequest> {
        match self {
            Self::Invalid(reason) => Some(reason),
            Self::Valid | Self::Retryable => None,
        }
    }
}

impl From<OrderRequestVerification> for bool {
    fn from(verification: OrderRequestVerification) -> Self {
        verification.is_valid()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidOrderRequest {
    UnknownToken,
    InactiveToken,
    MissingScope,
    MalformedSignature,
    ExpiredSignature,
    FutureSignature,
    InvalidValidity,
    SignatureMismatch,
    Replay,
}

impl InvalidOrderRequest {
    pub fn detail(self) -> &'static str {
        match self {
            Self::UnknownToken => "unknown token",
            Self::InactiveToken => "inactive token",
            Self::MissingScope => "missing order write scope",
            Self::MalformedSignature => "malformed signature",
            Self::ExpiredSignature => "expired signature",
            Self::FutureSignature => "signature issued in the future",
            Self::InvalidValidity => "signature validity exceeds policy",
            Self::SignatureMismatch => "signature mismatch",
            Self::Replay => "replayed signature",
        }
    }
}
