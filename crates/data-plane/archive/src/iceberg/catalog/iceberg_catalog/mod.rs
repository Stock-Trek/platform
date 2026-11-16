mod catalog_metadata;
mod claim_index;
mod claim_index_lookup;
mod claim_index_page;
mod commit;
mod commit_algorithm;
mod rest_catalog;
mod rest_connection;
mod retention;
mod scan;
mod snapshot;
mod snapshot_file;
mod table_contract;
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
