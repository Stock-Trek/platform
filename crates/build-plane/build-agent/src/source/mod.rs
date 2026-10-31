//! Download and unpack the user's algorithm sources.
//!
//! The orchestrator uploads a zip archive of the repository's
//! `src/algorithms` directory and passes the pre-signed download URL in the
//! build request as `signed_get_url`. Archive paths are rooted at
//! `crates/sandbox-plane/runner/src/user_generated/` so extracting the archive over the
//! working directory replaces the checked in algorithm stub with the user's
//! source.

mod source;

#[cfg(test)]
mod tests;

pub use source::*;
