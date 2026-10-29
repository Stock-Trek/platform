//! Minimal Iceberg REST catalog support for archive files.
//!
//! The archive services write Parquet objects directly to S3. Iceberg is the
//! source of truth for which of those objects are durable: a Parquet object is
//! only visible to readers after a snapshot commit. The implementation here
//! emits format-version 2 manifest and manifest-list files and commits append
//! snapshots through the Iceberg REST catalog protocol.

mod avro;
mod catalog;
mod config;
mod metadata;
mod schema;

pub use catalog::{ArchiveCatalog, IcebergCatalog};
pub use config::IcebergConfig;
pub use metadata::IcebergDataFile;
pub use schema::{ArchiveIcebergTable, ArchiveTableLayer};
