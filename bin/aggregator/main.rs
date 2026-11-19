use platform_aggregator::AggregatorApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    AggregatorApp::run().await
}
