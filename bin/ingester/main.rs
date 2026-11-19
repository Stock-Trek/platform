use platform_errors::PlatformResult;
use platform_ingester::IngesterApp;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    IngesterApp::run().await
}
