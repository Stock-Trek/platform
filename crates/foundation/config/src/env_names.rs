//! Environment variable names shared by more than one crate.
//!
//! Process-specific names stay next to the configuration type that reads them.
//! Names in this module are part of a contract between processes (or between a
//! process and a shared integration), so both sides must agree on the spelling.

/// Tracing filter read by every process.
pub const RUST_LOG: &str = "RUST_LOG";

/// AWS SDK connection timeout, in seconds.
pub const AWS_CONNECT_TIMEOUT_SECS: &str = "AWS_CONNECT_TIMEOUT_SECS";

/// Name of the Lambda function currently handling an event.
pub const AWS_LAMBDA_FUNCTION_NAME: &str = "AWS_LAMBDA_FUNCTION_NAME";

/// AWS SDK operation timeout, in seconds.
pub const AWS_OPERATION_TIMEOUT_SECS: &str = "AWS_OPERATION_TIMEOUT_SECS";

/// Maximum time the build agent may spend downloading source, in seconds.
pub const BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS: &str =
    "BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS";

/// Maximum time the build agent may spend on a build, in seconds.
pub const BUILD_AGENT_TIMEOUT_SECS: &str = "BUILD_AGENT_TIMEOUT_SECS";

/// Time reserved for cleanup before a Lambda invocation deadline, in seconds.
pub const DEADLINE_BUFFER_SECS: &str = "DEADLINE_BUFFER_SECS";

/// Comma-separated list of GitHub hosts allowed for outbound requests.
pub const GITHUB_ENTERPRISE_ALLOWED_HOSTS: &str = "GITHUB_ENTERPRISE_ALLOWED_HOSTS";

/// Maximum accepted source archive size, in bytes.
pub const S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES: &str = "S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES";

/// Lifetime of S3 signed URLs, in seconds.
pub const S3_SIGNED_URL_EXPIRY_SECS: &str = "S3_SIGNED_URL_EXPIRY_SECS";

/// URL of the SQS queue that carries build requests.
pub const SQS_QUEUE_URL: &str = "SQS_QUEUE_URL";
