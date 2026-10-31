//! Upload build artifacts to S3 using a pre-signed POST policy.
//!
//! The orchestrator sends a short-lived pre-signed POST in the build request
//! so the agent can write its artifacts without any long-lived AWS credentials.
//! The url and form fields are the `signed_post.url` and `signed_post.fields`
//! properties of the Lambda invocation payload. The fields hold the policy
//! values that must be sent alongside the file (for example `key`, `policy`,
//! `x-amz-signature`, ...).
//!
//! A single pre-signed POST can upload more than one artifact when the `key`
//! field contains the `${filename}` variable, which S3 replaces with the name
//! of the uploaded file.

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::*;
