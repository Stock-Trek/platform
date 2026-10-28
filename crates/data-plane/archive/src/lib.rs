//! Shared archive records, Parquet encoding, and archive batching.
//!
//! The archiver writes these records and the compactor reads them back, so the
//! record shape, deduplication keys, partitioning, and Parquet schema live here
//! instead of being reimplemented by each service.

pub mod buffer;
pub mod layout;
pub mod parquet;
pub mod record;
pub mod source;

pub use buffer::ArchiveBuffer;
pub use layout::{ArchiveBucket, ArchiveHour, ArchiveLayout, ArchivePath, ArchiveTimePartition};
pub use parquet::{ArchiveParquet, ArchiveParquetBatches, ArchiveParquetWriter};
pub use record::{ArchiveKind, ArchivePartition, ArchiveRecord, ArchiveRecordKey};
pub use source::SourceBatch;
