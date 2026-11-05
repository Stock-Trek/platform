//! Download a compiled runner artifact from a pre-signed URL.
//!
//! The control plane signs an S3 `GetObject` request and passes the URL in the
//! run request as `cwasmGetUrl`. The artifact is streamed to a private
//! temporary file so it does not need to fit in memory.

mod implementation;
mod runtime;
mod workspace;

#[cfg(test)]
mod runtime_tests;

#[cfg(test)]
mod tests;

pub use implementation::*;
pub use runtime::*;
pub use workspace::*;
