//! DynamoDB persistence for scheduled algorithm runs.
//!
//! The control plane keeps one item per scheduled run so other services can
//! answer which algorithms get run and when. Items follow this schema:
//!
//! - Partition key `algorithmId` (string) identifies the algorithm.
//! - Sort key `scheduledAt` (string, RFC 3339) identifies the scheduled run.
//! - `statusIndex` is a global secondary index on `status` (string) and
//!   `scheduledAt`, projecting all attributes, so due runs can be found by
//!   querying pending entries.
//!
//! Every item also carries the details needed to invoke the runner: the S3
//! key of the compiled cwasm artifact, its SHA-256 digest, size, Wasmtime
//! version, the strategy name to run, and the algorithm owner whose auth-service
//! secret signs the resulting order requests.

mod cwasm_artifact;
mod run_record;
mod run_status;
mod run_store;

#[cfg(test)]
mod tests;

pub use cwasm_artifact::CwasmArtifact;
pub use run_record::RunRecord;
pub use run_status::RunStatus;
pub use run_store::{RUN_STATUS_INDEX, RunStore};
