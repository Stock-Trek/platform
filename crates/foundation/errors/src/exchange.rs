use crate::{bp::BpError, retryability::Retryability};
use platform_types::{new_types::DurationSeconds, protocol::Protocol};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ExchangeError {
    #[error("Cannot send request {request_type} on protocol {protocol}")]
    BadProtocol {
        request_type: String,
        protocol: Protocol,
    },
    #[error("Bad response from exchange. {0}")]
    BadResponse(String),
    #[error("Cannot use {name}={value}. {reason}")]
    BadValue {
        name: String,
        value: String,
        reason: String,
    },
    #[error("Crypto key error: {0}")]
    CryptoKey(String),
    #[error("Deserialize response error: {0}")]
    DeserializeResponse(serde_json::Error),
    #[error("Exchange request failed while trying to {operation}: {source}")]
    Exchange {
        operation: &'static str,
        #[source]
        source: Box<BpError>,
    },
    #[error("Cannot find a symbol for trading pair {trading_pair}")]
    MissingSymbol { trading_pair: String },
    #[error("Parsing error: {0}")]
    ParseError(String),
    #[error("Exchange response failed with status {status} and code {code}: {message}")]
    Response {
        code: i64,
        message: String,
        status: u16,
        retry_after: Option<DurationSeconds>,
        retryability: Retryability,
    },
    #[error("Serialize request error: {0}")]
    SerializeRequest(serde_json::Error),
    #[error("Unknown {exchange} submission outcome while fetching klines")]
    UnknownSubmissionOutcome { exchange: &'static str },
    #[error("Value for {0} deserialized as Unknown")]
    UnknownValue(String),
    #[error("Unsupported exchange: {exchange}")]
    UnsupportedExchange { exchange: String },
    #[error("Unsupported trading mode: {trading_mode}")]
    UnsupportedTradingMode { trading_mode: String },
}

impl ExchangeError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Response { retryability, .. } => *retryability,
            Self::Exchange { source, .. } => source.retryability(),
            _ => Retryability::Permanent,
        }
    }
}
