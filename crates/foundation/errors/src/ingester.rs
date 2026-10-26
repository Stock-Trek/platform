use crate::retryability::Retryability;

/// Errors raised while decoding exchange stream frames or running the ingester.
#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum IngesterError {
    #[error("expected a JSON object")]
    NotAnObject,
    #[error("expected message payload to be a JSON object")]
    PayloadNotAnObject,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("field {0} has the wrong type")]
    InvalidField(&'static str),
    #[error("sequenced event is missing sequence field {field}")]
    MissingSequence { field: &'static str },
    #[error("unsupported stream format: {format}")]
    UnsupportedStreamFormat { format: String },
    #[error("malformed SBE frame from {exchange}: {message}")]
    MalformedSbeFrame { exchange: String, message: String },
    #[error("unknown SBE template id {template_id} from {exchange}")]
    UnknownSbeTemplate { exchange: String, template_id: u16 },
    #[error("failed to decode {exchange} JSON frame: {source}")]
    Json {
        exchange: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("frame from {exchange} is not valid UTF-8: {source}")]
    Utf8 {
        exchange: String,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("NATS protocol error: {message}")]
    Protocol { message: String },
}

impl IngesterError {
    pub fn malformed_sbe(exchange: impl Into<String>, message: impl Into<String>) -> Self {
        Self::MalformedSbeFrame {
            exchange: exchange.into(),
            message: message.into(),
        }
    }

    pub fn unknown_sbe_template(exchange: impl Into<String>, template_id: u16) -> Self {
        Self::UnknownSbeTemplate {
            exchange: exchange.into(),
            template_id,
        }
    }

    pub fn json(exchange: impl Into<String>, source: serde_json::Error) -> Self {
        Self::Json {
            exchange: exchange.into(),
            source,
        }
    }

    pub fn utf8(exchange: impl Into<String>, source: std::str::Utf8Error) -> Self {
        Self::Utf8 {
            exchange: exchange.into(),
            source,
        }
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        Retryability::Permanent
    }
}
