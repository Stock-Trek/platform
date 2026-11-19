use platform_build_orchestrator::BuildOrchestratorApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    BuildOrchestratorApp::run().await
}
