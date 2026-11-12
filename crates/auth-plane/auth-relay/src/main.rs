use platform_auth_relay::AuthRelayApp;
use platform_errors::BuildResult;

#[tokio::main]
async fn main() -> BuildResult<()> {
    AuthRelayApp::run().await
}
