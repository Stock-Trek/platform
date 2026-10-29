//! Paths shared by the build plane, derived from the cargo workspace metadata.

pub mod runner {
    include!(concat!(env!("OUT_DIR"), "/paths.rs"));
}

/// Directory in a user's repository that contains their algorithm sources.
pub const USER_SOURCE_DIRECTORY: &str = "src/user_generated";
