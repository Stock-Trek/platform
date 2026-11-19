use platform_build_agent::BuildAgentApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    BuildAgentApp::run().await
}
