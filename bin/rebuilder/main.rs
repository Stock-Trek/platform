use platform_errors::PlatformResult;
use platform_rebuilder::RebuilderApp;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    RebuilderApp::run().await
}
