use crate::websocket_listener::response_handler::ResponseHandler;
use parking_lot::Mutex;
use std::sync::Arc;

pub(crate) type Handlers = Arc<Mutex<Vec<ResponseHandler>>>;
