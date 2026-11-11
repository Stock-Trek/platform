use super::{
    vault_http_tests::VaultHttp,
    vault_kv_state_tests::{CapturedRequest, VaultKvState},
};
use serde_json::json;
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

pub(crate) struct MockVault {
    address: SocketAddr,
    state: Arc<Mutex<VaultKvState>>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockVault {
    pub(crate) fn start(mount: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mount = mount.to_string();
        let state = Arc::new(Mutex::new(VaultKvState::default()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let state = state.clone();
            let requests = requests.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                for incoming in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(stream) = incoming else {
                        break;
                    };
                    Self::handle(stream, &mount, &state, &requests);
                }
            })
        };
        Self {
            address,
            state,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) fn requests(&self) -> Vec<CapturedRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub(crate) fn seed(&self, key: &str) {
        self.state.lock().unwrap().seed(key);
    }

    pub(crate) fn current(&self, id: &str) -> Option<(serde_json::Value, u64)> {
        self.state.lock().unwrap().read(id)
    }

    fn handle(
        mut stream: TcpStream,
        mount: &str,
        state: &Mutex<VaultKvState>,
        requests: &Mutex<Vec<CapturedRequest>>,
    ) {
        let Some(request) = VaultHttp::read_request(&mut stream) else {
            return;
        };
        let (path, query) = match request.target.split_once('?') {
            Some((path, query)) => (path, Some(query.to_string())),
            None => (request.target.as_str(), None),
        };
        requests.lock().unwrap().push(CapturedRequest {
            method: request.method.clone(),
            path: path.to_string(),
            query: query.clone(),
            token: request.token,
            body: request.body.clone(),
        });
        let prefix = format!("/v1/{mount}");
        let route = path.strip_prefix(&prefix).unwrap_or_default();
        let route = route.trim_start_matches('/');
        let (area, key) = route.split_once('/').unwrap_or((route, ""));
        let mut state = state.lock().unwrap();
        let (status, body) = match (request.method.as_str(), area, key, query.as_deref()) {
            ("PUT", "data", id, _) if !id.is_empty() => state.put_response(id, &request.body),
            ("GET", "data", id, _) if !id.is_empty() => state.get_response(id),
            ("DELETE", "metadata", id, _) if !id.is_empty() => {
                state.delete(id);
                (204, json!({}))
            }
            ("GET", "metadata", directory, Some("list=true")) => state.list_response(directory),
            _ => (404, json!({ "errors": [] })),
        };
        VaultHttp::respond(&mut stream, status, body);
    }
}

impl Drop for MockVault {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
