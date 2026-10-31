//! The payload sent from the orchestrator to the build agent Lambda.
//!
//! The payload is the Lambda invocation event, so it contains everything that
//! changes per build. Process-wide settings remain in the agent's environment.

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::*;
