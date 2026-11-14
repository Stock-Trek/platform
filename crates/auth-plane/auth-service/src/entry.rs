use crate::{AuthServiceRunner, JwtServerConfig};
use platform_errors::PlatformResult;

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

    fn execute() -> PlatformResult<()> {
        let config = JwtServerConfig::from_env()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(AuthServiceRunner::run(config))
    }
}
