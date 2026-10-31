//! Hand-rolled SigV4 presigned POST signing.
//!
//! The AWS SDK can presign individual S3 requests, but it does not expose the
//! browser-style POST policy used by the build agent. Keep that signing logic
//! isolated here so ordinary S3 operations can use the SDK client.

mod presigned_post;

#[cfg(test)]
mod tests;

pub use presigned_post::*;
