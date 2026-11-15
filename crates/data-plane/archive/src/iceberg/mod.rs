mod archive_iceberg_table;
mod catalog;
mod config;
mod registration;

#[cfg(test)]
mod archive_iceberg_table_tests;
#[cfg(test)]
mod registration_tests;

pub use archive_iceberg_table::{ArchiveIcebergTable, ArchiveTableLayer};
pub use catalog::{ArchiveCatalog, IcebergCatalog, IcebergDataFile};
pub use config::{
    ICEBERG_CATALOG_TOKEN, ICEBERG_CATALOG_URI, ICEBERG_CATALOG_WAREHOUSE, ICEBERG_NAMESPACE,
    IcebergConfig,
};
pub use registration::{ArchiveRegistration, ConflictDetails, RegisterOutcome};
