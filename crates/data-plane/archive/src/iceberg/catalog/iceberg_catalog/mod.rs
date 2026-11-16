mod catalog_metadata;
mod commit;
mod rest_catalog;
mod scan;
mod snapshot;

#[cfg(test)]
mod catalog_metadata_tests;

pub use rest_catalog::IcebergCatalog;
