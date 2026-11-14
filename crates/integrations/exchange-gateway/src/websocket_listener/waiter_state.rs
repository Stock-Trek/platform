use platform_errors::PlatformError;
use std::task::Waker;

#[derive(Default)]
pub struct WaiterState {
    pub filtered_response: Option<serde_json::Value>,
    pub error: Option<PlatformError>,
    pub waker: Option<Waker>,
}
