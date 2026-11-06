//! Presigned POST signing for build artifacts.
//!
//! The SigV4 POST-policy signing itself lives in `platform-aws`; this module
//! wraps it with the build plane's artifact key template and size limit.

mod post_policy;

#[cfg(test)]
mod tests;

pub use post_policy::*;
