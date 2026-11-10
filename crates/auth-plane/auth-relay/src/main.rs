use platform_auth_relay::AuthRelayApp;
use platform_errors::BpResult;

#[tokio::main]
async fn main() -> BpResult<()> {
    AuthRelayApp::run().await
}
