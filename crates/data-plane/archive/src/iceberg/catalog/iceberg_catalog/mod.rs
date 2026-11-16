mod catalog_metadata;
mod commit;
mod commit_algorithm;
mod rest_catalog;
mod retention;
mod scan;
mod snapshot;
mod table_properties;

#[cfg(test)]
mod catalog_metadata_tests;
#[cfg(test)]
mod commit_crash_tests;
#[cfg(test)]
mod commit_fake_tests;
#[cfg(test)]
mod commit_tests;
#[cfg(test)]
mod conditional_commit_tests;

pub use rest_catalog::IcebergCatalog;
