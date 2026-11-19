use platform_errors::PlatformResult;
use platform_git_webhook::GitWebhookApp;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    GitWebhookApp::run().await
}
