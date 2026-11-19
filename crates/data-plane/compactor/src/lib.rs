mod app;
mod compactor;
mod compactor_runner;
mod config;
mod object_lister;
mod partition_output;
mod source;
mod source_inventory;
mod spill;

#[cfg(test)]
mod compactor_runner_tests;
#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod source_tests;

pub use app::CompactorApp;
