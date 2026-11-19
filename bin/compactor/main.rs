use platform_compactor::CompactorApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    CompactorApp::run().await
}
