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
    #[error("invalid sequence field {field}: {reason}")]
    InvalidSequence {
        field: &'static str,
        reason: &'static str,
    },
    #[error("unsupported stream format: {format}")]
    UnsupportedStreamFormat { format: String },
    #[error("malformed binary frame: {message}")]
    MalformedBinaryFrame { message: String },
    #[error("malformed SBE frame: {message}")]
    MalformedSbeFrame { message: String },
    #[error("unknown SBE template id {template_id}")]
    UnknownSbeTemplate { template_id: u16 },
    #[error("failed to decode JSON frame: {source}")]
    Json {
        #[source]
        source: serde_json::Error,
    },
    #[error("frame is not valid UTF-8: {source}")]
    Utf8 {
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("NATS protocol error: {message}")]
    Protocol { message: String },
}

impl IngesterError {
    pub fn malformed_binary(message: impl Into<String>) -> Self {
        Self::MalformedBinaryFrame {
            message: message.into(),
        }
    }

    pub fn malformed_sbe(message: impl Into<String>) -> Self {
        Self::MalformedSbeFrame {
            message: message.into(),
        }
    }

    pub fn unknown_sbe_template(template_id: u16) -> Self {
        Self::UnknownSbeTemplate { template_id }
    }

    pub fn json(source: serde_json::Error) -> Self {
        Self::Json { source }
    }

    pub fn utf8(source: std::str::Utf8Error) -> Self {
        Self::Utf8 { source }
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
