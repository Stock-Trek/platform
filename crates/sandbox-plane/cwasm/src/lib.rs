//! Download a compiled runner artifact from a pre-signed URL.
//!
//! The control plane signs an S3 `GetObject` request and passes the URL in the
//! run request as `cwasmGetUrl`. The artifact is streamed to a private
//! temporary file so it does not need to fit in memory.
//!
//! This crate is part of the native sandbox plane. It intentionally has no
//! implementation on `wasm32`, where the runner is built for sandboxed
//! execution rather than hosted.

#[cfg(not(target_arch = "wasm32"))]
mod implementation;

#[cfg(not(target_arch = "wasm32"))]
pub use implementation::*;
