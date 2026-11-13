//! Shared AWS configuration and client construction.
//!
//! Services should load [`SdkConfig`] through this crate instead of calling
//! `aws_config` directly, so timeouts, behaviour version, and client options
//! stay consistent across the workspace.

mod aws_client;
mod presigned_post;
mod ssm_config;
mod ssm_secrets;

#[cfg(test)]
mod presigned_post_tests;
#[cfg(test)]
mod ssm_secrets_tests;

pub use aws_client::Aws;
pub use aws_config::{Region, SdkConfig};
pub use presigned_post::{PresignedPost, PresignedPostSigner};
pub use ssm_config::{SSM_PATH_ENV, SsmConfig};
pub use ssm_secrets::{SSM_DECRYPT_ENV, SsmSecrets};
