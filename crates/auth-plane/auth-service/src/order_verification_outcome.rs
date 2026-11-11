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
