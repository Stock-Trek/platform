use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("cargo always sets CARGO_MANIFEST_DIR"),
    );
    let lock_path = manifest_dir.join("../../../Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock_path.display());

    let lock = fs::read_to_string(&lock_path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", lock_path.display()));

    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "name = \"wasmtime\"" {
            continue;
        }
        let version_line = lines
            .next()
            .unwrap_or_else(|| panic!("missing version for wasmtime in {}", lock_path.display()));
        let version = version_line
            .trim()
            .strip_prefix("version = \"")
            .and_then(|version| version.strip_suffix('"'))
            .unwrap_or_else(|| panic!("invalid wasmtime version line in {}", lock_path.display()));
        println!("cargo:rustc-env=PLATFORM_WASMTIME_VERSION={version}");
        return;
    }

    panic!(
        "failed to find the wasmtime dependency in {}",
        lock_path.display()
    );
}
