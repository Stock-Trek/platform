use platform_errors::PlatformResult;
use platform_run_invoker::RunInvokerApp;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    RunInvokerApp::run().await
}
