use platform_backfiller::BackfillerApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    BackfillerApp::run().await
}
