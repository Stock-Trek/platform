use std::path::Path;

pub struct ProtoGen;

impl ProtoGen {
    /// Generates Rust DTOs from the .proto files in `proto_dir`,
    /// writing the resulting `.rs` files into `out_dir`.
    pub fn generate(proto_dir: &Path, out_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if out_dir.exists() {
            std::fs::remove_dir_all(out_dir)?;
        }
        std::fs::create_dir_all(out_dir)?;

        let mut protos: Vec<_> = std::fs::read_dir(proto_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "proto"))
            .collect();
        protos.sort();

        let protoc = protoc_bin_vendored::protoc_bin_path()?;

        prost_build::Config::new()
            .protoc_executable(protoc)
            .out_dir(out_dir)
            .compile_protos(&protos, &[proto_dir])?;

        let dto_path = out_dir.join("dto.rs");
        let generated = std::fs::read_to_string(&dto_path)?;
        std::fs::write(
            &dto_path,
            format!("#![cfg_attr(rustfmt, rustfmt::skip)]\n{generated}"),
        )?;

        let mod_path = out_dir.join("mod.rs");
        let contents = "pub mod dto;\n";
        std::fs::write(mod_path, contents)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_sources_match_proto_files() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .expect("proto-gen lives under the workspace root");
        let out_dir = std::env::temp_dir().join(format!("proto-gen-test-{}", std::process::id()));
        ProtoGen::generate(&root.join("proto"), &out_dir).expect("failed to generate DTOs");

        for name in ["dto.rs", "mod.rs"] {
            let expected = std::fs::read_to_string(
                root.join("crates/sandbox-plane/runner/src/generated")
                    .join(name),
            )
            .unwrap_or_else(|error| panic!("failed to read checked-in {name}: {error}"));
            let actual = std::fs::read_to_string(out_dir.join(name))
                .unwrap_or_else(|error| panic!("failed to read generated {name}: {error}"));
            assert_eq!(
                expected, actual,
                "crates/sandbox-plane/runner/src/generated/{name} is out of date; run `cargo run -p platform-proto-gen`"
            );
        }

        std::fs::remove_dir_all(&out_dir).ok();
    }
}
