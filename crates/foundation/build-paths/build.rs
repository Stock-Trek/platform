use cargo_metadata::MetadataCommand;
use std::{env, fs, path::Path};

const RUNNER_PACKAGE: &str = "platform-runner";

fn relative_path(workspace_root: &Path, path: &Path) -> String {
    path.strip_prefix(workspace_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn main() {
    let metadata = MetadataCommand::new()
        .no_deps()
        .exec()
        .expect("read cargo workspace metadata");
    let workspace_root = metadata.workspace_root.as_std_path();
    let runner = metadata
        .packages
        .iter()
        .find(|package| package.name.as_str() == RUNNER_PACKAGE)
        .expect("platform-runner is a workspace member");
    let manifest = runner.manifest_path.as_std_path();
    let directory = manifest
        .parent()
        .expect("platform-runner manifest has a parent directory");
    let user_generated = relative_path(workspace_root, &directory.join("src/user_generated"));
    let generated = relative_path(workspace_root, &directory.join("src/generated"));
    let manifest = relative_path(workspace_root, manifest);
    let directory = relative_path(workspace_root, directory);

    let output = format!(
        "pub const PACKAGE: &str = {RUNNER_PACKAGE:?};\n\
         pub const MANIFEST: &str = {manifest:?};\n\
         pub const DIR: &str = {directory:?};\n\
         pub const USER_GENERATED_DIR: &str = {user_generated:?};\n\
         pub const USER_GENERATED_PREFIX: &str = {prefix:?};\n\
         pub const GENERATED_DIR: &str = {generated:?};\n",
        prefix = format!("{user_generated}/"),
    );
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is set");
    fs::write(Path::new(&out_dir).join("paths.rs"), output).expect("write generated runner paths");
    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("Cargo.toml").display()
    );
    println!("cargo:rerun-if-changed={}", runner.manifest_path);
}
