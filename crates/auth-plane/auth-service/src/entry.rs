use crate::{AuthServiceRunner, JwtServerConfig};
use platform_errors::BpResult;

pub struct Entry;

impl Entry {
    pub fn run() -> i32 {
        match Self::execute() {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("auth-service: {error}");
                1
            }
        }
    }

    fn execute() -> BpResult<()> {
        let config = JwtServerConfig::from_env()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(AuthServiceRunner::run(config))
    }
}
