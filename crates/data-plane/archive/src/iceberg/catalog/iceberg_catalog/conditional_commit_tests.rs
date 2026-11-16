use super::IcebergCatalog;
use crate::iceberg::{ArchiveIcebergTable, IcebergConfig};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::Duration;

struct CatalogServer {
    address: String,
    requests: Receiver<Value>,
}

impl CatalogServer {
    fn start(expected_uuid: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let expected_uuid = expected_uuid.to_string();
        let (sender, requests) = channel();
        thread::spawn(move || {
            for _ in 0..3 {
                let (stream, _) = listener.accept().unwrap();
                Self::serve(stream, &expected_uuid, &sender);
            }
        });
        Self { address, requests }
    }

    fn serve(mut stream: TcpStream, expected_uuid: &str, sender: &Sender<Value>) {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let request = Self::read_request(&mut stream);
        if request.starts_with("GET ") {
            Self::respond(&mut stream, 200, r#"{"defaults":{},"overrides":{}}"#);
            return;
        }
        let body = request
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or_default();
        let body: Value = serde_json::from_str(body).unwrap();
        sender.send(body.clone()).unwrap();
        let uuid = body["requirements"][0]["uuid"].as_str().unwrap_or_default();
        if uuid == expected_uuid {
            Self::respond(&mut stream, 200, "{}");
        } else {
            Self::respond(&mut stream, 409, r#"{"error":"stale table uuid"}"#);
        }
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut received = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buffer[..read]);
            if let Some((headers, body)) = Self::split_http(&received) {
                let content_length = Self::content_length(headers);
                if body.len() >= content_length {
                    return String::from_utf8_lossy(&received).to_string();
                }
            }
        }
        String::from_utf8_lossy(&received).to_string()
    }

    fn split_http(bytes: &[u8]) -> Option<(&str, &[u8])> {
        let separator = b"\r\n\r\n";
        let index = bytes
            .windows(separator.len())
            .position(|window| window == separator)?;
        let headers = std::str::from_utf8(&bytes[..index]).ok()?;
        Some((headers, &bytes[index + separator.len()..]))
    }

    fn content_length(headers: &str) -> usize {
        headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0)
    }

    fn respond(stream: &mut TcpStream, status: u16, body: &str) {
        let reason = if status == 200 { "OK" } else { "Conflict" };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    }
}

impl CatalogServer {
    fn commit_request(uuid: &str) -> Value {
        json!({
            "requirements": [
                {"type": "assert-table-uuid", "uuid": uuid},
                {"type": "assert-ref-snapshot-id", "ref": "main"}
            ],
            "updates": []
        })
    }
}

#[tokio::test]
async fn a_stale_table_uuid_is_rejected_and_the_request_is_conditional() {
    let expected = "11111111-1111-4111-8111-111111111111";
    let server = CatalogServer::start(expected);
    let catalog = IcebergCatalog::connect(&IcebergConfig {
        catalog_uri: server.address.clone(),
        warehouse: "s3://archive/warehouse".to_string(),
        token: None,
        namespace: "ns".to_string(),
    })
    .await
    .unwrap();
    let stale = CatalogServer::commit_request("22222222-2222-4222-8222-222222222222");

    let error = catalog
        .send_commit(ArchiveIcebergTable::RawEvents, &stale)
        .await
        .unwrap_err();
    let sent = server.requests.recv().unwrap();

    assert!(error.to_string().contains("409"));
    assert_eq!(sent["requirements"][0]["type"], "assert-table-uuid");
    assert_eq!(sent["requirements"][1]["type"], "assert-ref-snapshot-id");

    catalog
        .send_commit(
            ArchiveIcebergTable::RawEvents,
            &CatalogServer::commit_request(expected),
        )
        .await
        .unwrap();
    assert_eq!(
        server.requests.recv().unwrap(),
        CatalogServer::commit_request(expected)
    );
}
