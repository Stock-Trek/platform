//! Paths shared by the build plane, derived from the cargo workspace metadata.

pub mod runner {
    include!(concat!(env!("OUT_DIR"), "/paths.rs"));
}

/// Directory in a user's repository that contains their algorithm sources.
pub const USER_SOURCE_DIRECTORY: &str = "src/user_generated";

#[cfg(test)]
mod tests {
    use super::runner;
    use std::path::Path;

    #[test]
    fn runner_paths_point_to_the_checked_in_runner_tree() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("build-paths lives under the workspace root");

        assert!(workspace.join(runner::MANIFEST).is_file());
        assert!(workspace.join(runner::USER_GENERATED_DIR).is_dir());
        assert!(workspace.join(runner::GENERATED_DIR).is_dir());
        assert_eq!(
            runner::USER_GENERATED_PREFIX,
            format!("{}/", runner::USER_GENERATED_DIR)
        );
    }
}
