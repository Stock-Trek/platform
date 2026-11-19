use platform_archiver::ArchiverApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    ArchiverApp::run().await
}
