use platform_errors::PlatformResult;
use platform_maintainer::MaintainerApp;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    MaintainerApp::run().await
}
