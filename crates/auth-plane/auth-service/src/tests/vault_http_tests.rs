use serde_json::Value;
use std::{
    io::{Read, Write},
    net::TcpStream,
};

pub(crate) struct RawRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) token: Option<String>,
    pub(crate) body: Value,
}

pub(crate) struct VaultHttp;

impl VaultHttp {
    pub(crate) fn read_request(stream: &mut TcpStream) -> Option<RawRequest> {
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
        let mut lines = headers.lines();
        let mut request_line = lines.next()?.split_whitespace();
        let method = request_line.next()?.to_string();
        let target = request_line.next()?.to_string();
        let token = lines.find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("x-vault-token")
                .then(|| value.trim().to_string())
        });
        let body = bytes
            .get(end..)
            .filter(|body| !body.is_empty())
            .map(serde_json::from_slice)
            .transpose()
            .ok()?
            .unwrap_or(Value::Null);
        Some(RawRequest {
            method,
            target,
            token,
            body,
        })
    }

    pub(crate) fn respond(stream: &mut TcpStream, status: u16, body: Value) {
        let reason = match status {
            200 => "OK",
            204 => "No Content",
            400 => "Bad Request",
            404 => "Not Found",
            _ => "Internal Server Error",
        };
        let body = if status == 204 {
            String::new()
        } else {
            body.to_string()
        };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
    }
}
