use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

pub(crate) struct CapturedSqsRequest {
    pub(crate) target: String,
    pub(crate) body: Value,
}

pub(crate) struct MockSqs {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedSqsRequest>>>,
    fail: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockSqs {
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let fail = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let requests = requests.clone();
            let fail = fail.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                for incoming in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(stream) = incoming else {
                        break;
                    };
                    Self::handle(stream, &requests, &fail);
                }
            })
        };
        Self {
            address,
            requests,
            fail,
            stop,
            thread: Some(thread),
        }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) fn requests(&self) -> Vec<CapturedSqsRequest> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| CapturedSqsRequest {
                target: request.target.clone(),
                body: request.body.clone(),
            })
            .collect()
    }

    pub(crate) fn fail_requests(&self) {
        self.fail.store(true, Ordering::SeqCst);
    }

    fn handle(mut stream: TcpStream, requests: &Mutex<Vec<CapturedSqsRequest>>, fail: &AtomicBool) {
        let Some(request) = Self::read_request(&mut stream) else {
            return;
        };
        let response = if fail.load(Ordering::SeqCst) {
            (
                500,
                json!({ "__type": "InternalError", "message": "queue unavailable" }),
            )
        } else {
            (200, Self::success_body(&request.target, &request.body))
        };
        requests.lock().unwrap().push(request);
        Self::respond(&mut stream, response.0, response.1);
    }

    fn read_request(stream: &mut TcpStream) -> Option<CapturedSqsRequest> {
        let mut bytes = Vec::new();
        let mut header_end = None;
        let mut content_length = 0;
        loop {
            if header_end.is_none()
                && let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n")
            {
                let end = position + 4;
                header_end = Some(end);
                let headers = String::from_utf8_lossy(&bytes[..end]);
                content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
            }
            if let Some(end) = header_end
                && bytes.len() >= end + content_length
            {
                break;
            }
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).ok()?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
        }
        let end = header_end?;
        let headers = String::from_utf8_lossy(&bytes[..end]);
        let target = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("x-amz-target")
                .then(|| value.trim().to_string())
        })?;
        let body = bytes
            .get(end..)
            .filter(|body| !body.is_empty())
            .map(serde_json::from_slice)
            .transpose()
            .ok()?
            .unwrap_or(Value::Null);
        Some(CapturedSqsRequest { target, body })
    }

    fn success_body(target: &str, request: &Value) -> Value {
        if target.ends_with(".SendMessageBatch") {
            let successful: Vec<Value> = request
                .get("Entries")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|entry| {
                    let id = entry.get("Id").cloned().unwrap_or(Value::Null);
                    json!({
                        "Id": id,
                        "MessageId": format!("message-{id}"),
                        "MD5OfMessageBody": "00000000000000000000000000000000",
                    })
                })
                .collect();
            json!({ "Successful": successful, "Failed": [] })
        } else {
            json!({
                "MessageId": "message-0",
                "MD5OfMessageBody": "00000000000000000000000000000000",
            })
        }
    }

    fn respond(stream: &mut TcpStream, status: u16, body: Value) {
        let reason = match status {
            200 => "OK",
            _ => "Internal Server Error",
        };
        let body = body.to_string();
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\n\
             Content-Type: application/x-amz-json-1.0\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
    }
}

impl Drop for MockSqs {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
