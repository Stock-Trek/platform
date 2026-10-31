//! Helpers for reading required environment variables.
//!
//! Environment variables are parsed by each process's own configuration type.
//! This module only provides the raw, fallible reads; it deliberately has no
//! knowledge of which variables any particular process needs.

mod env_vars;

#[cfg(test)]
mod tests;

pub use env_vars::*;
