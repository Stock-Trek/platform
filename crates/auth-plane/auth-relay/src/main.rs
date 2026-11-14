use platform_auth_relay::AuthRelayApp;
use platform_errors::PlatformResult;

#[tokio::main]
async fn main() -> PlatformResult<()> {
    AuthRelayApp::run().await
}
