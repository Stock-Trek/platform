mod app;
mod archiver_runner;
mod config;
mod recovery;
mod sink;
mod sink_registration;
mod source;

#[cfg(test)]
mod archiver_runner_tests;
#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod sink_registration_tests;

pub use app::ArchiverApp;
