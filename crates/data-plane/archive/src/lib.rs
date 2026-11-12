pub mod archive_buffer;
pub mod batch;

#[cfg(test)]
mod archive_buffer_tests;

pub mod archive_handoff;

#[cfg(test)]
mod archive_handoff_tests;

pub mod archive_parquet;
pub mod iceberg;
pub mod layout;

#[cfg(test)]
mod archive_parquet_tests;

pub mod parquet_reader;
pub mod quantization;

#[cfg(test)]
mod quantization_tests;

pub use quantization::*;
pub mod archive_record;

#[cfg(test)]
mod archive_record_tests;

pub mod routing_key;

#[cfg(test)]
mod routing_key_tests;

pub mod s3_object;

#[cfg(test)]
mod s3_object_tests;

pub mod source;
pub mod spill;

pub use archive_buffer::ArchiveBuffer;
pub use archive_handoff::ArchiveHandoff;
pub use archive_parquet::{
    ArchiveParquet, ArchiveParquetBatches, ArchiveParquetStream, ArchiveParquetWriter,
};
pub use archive_record::{
    ArchiveKind, ArchivePartition, ArchiveRecord, ArchiveRecordKey, ArchiveSortKey,
    ArchivedMarketEvent, STATE_EPOCH_HEADER,
};
pub use batch::ArchiveBatch;
pub use iceberg::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveTableLayer, ICEBERG_CATALOG_TOKEN,
    ICEBERG_CATALOG_URI, ICEBERG_CATALOG_WAREHOUSE, ICEBERG_NAMESPACE, IcebergCatalog,
    IcebergConfig, IcebergDataFile,
};
pub use layout::{ArchiveBucket, ArchiveHour, ArchivePath};
pub use parquet_reader::ParquetReader;
pub use routing_key::{ROUTING_BUCKET_COUNT, RoutingBucket, RoutingKey};
pub use s3_object::{Bucket, ObjectKey, S3, S3Object};
pub use source::SourceBatch;
pub use spill::{SpillRun, SpillRunReader};
