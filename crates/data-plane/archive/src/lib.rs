//! Shared archive records, Parquet encoding, storage helpers, and planning.
//!
//! The archiver writes these records and the compactor reads them back, so the
//! record shape, deduplication keys, partitioning, Parquet schema, object-key
//! layout, and storage operations live here instead of being reimplemented by
//! each service.

pub mod buffer;
pub mod constants;
pub mod layout;
pub mod parquet;
pub mod parquet_reader;
pub mod parquet_writer;
pub mod plan;
pub mod quantized_time;
pub mod record;
pub mod s3;
pub mod source;
pub mod subfolders;

pub use buffer::ArchiveBuffer;
pub use layout::{ArchiveBucket, ArchiveHour, ArchiveLayout, ArchivePath, ArchiveTimePartition};
pub use parquet::{ArchiveParquet, ArchiveParquetBatches, ArchiveParquetWriter};
pub use parquet_reader::ParquetReader;
pub use parquet_writer::writer_properties;
pub use record::{ArchiveKind, ArchivePartition, ArchiveRecord, ArchiveRecordKey};
pub use s3::{Bucket, ObjectKey, S3};
pub use source::SourceBatch;
