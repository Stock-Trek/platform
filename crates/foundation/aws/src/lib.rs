//! Shared AWS configuration and client construction.
//!
//! Services should load [`SdkConfig`] through this crate instead of calling
//! `aws_config` directly, so timeouts, behaviour version, and client options
//! stay consistent across the workspace.

mod aws_client;
pub mod presigned_post;
pub mod secrets;

#[cfg(test)]
mod presigned_post_tests;

pub use aws_client::Aws;
pub use aws_config::{Region, SdkConfig};
pub use presigned_post::{PresignedPost, PresignedPostSigner};
pub use secrets::{SSM_DECRYPT_ENV, SSM_PATH_ENV, Secrets, SsmConfig};
