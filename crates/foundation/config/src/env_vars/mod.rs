//! Helpers for reading required environment variables.
//!
//! Environment variables are parsed by each process's own configuration type.
//! This module only provides the raw, fallible reads; it deliberately has no
//! knowledge of which variables any particular process needs.

mod environment_variables;

#[cfg(test)]
mod tests;

pub use environment_variables::*;
