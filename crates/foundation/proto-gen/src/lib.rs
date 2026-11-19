mod app;
mod proto_gen;

#[cfg(test)]
mod proto_gen_tests;

pub use app::ProtoGenApp;
pub use proto_gen::{GENERATED_DIR, ProtoGen};
