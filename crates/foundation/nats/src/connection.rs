//! Core NATS protocol connection.

use crate::{
    NatsAddress, NatsSubject,
    message::{Headers, Message},
};
use platform_errors::{BpError, BpResult, IngesterError};
use serde_json::{Map, Value, json};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    time::{Instant, timeout_at},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

static CONNECTION_NONCE: AtomicU64 = AtomicU64::new(0);

/// A single NATS protocol connection speaking INFO/CONNECT, PUB/HPUB, SUB/UNSUB
/// and MSG/HMSG. Request/response semantics are provided on top of a subject
/// subscription used as an inbox.
pub(crate) struct NatsConnection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    inbox_nonce: u64,
    inbox_counter: u64,
    next_sid: u64,
}

impl NatsConnection {
    pub(crate) async fn connect(address: &NatsAddress, name: &str) -> BpResult<Self> {
        let stream = address.tcp_connect().await?;
        let (read_half, write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let info = Self::read_protocol_line(&mut reader)
            .await?
            .ok_or_else(|| Self::protocol_error("NATS connection closed during handshake"))?;
        if !info.starts_with("INFO ") {
            return Err(Self::protocol_error(format!(
                "expected INFO from NATS server, got {info:?}"
            )));
        }

        let mut connect = Map::new();
        connect.insert("verbose".into(), Value::Bool(false));
        connect.insert("pedantic".into(), Value::Bool(false));
        connect.insert("tls_required".into(), Value::Bool(false));
        connect.insert("name".into(), Value::String(name.to_string()));
        connect.insert("lang".into(), Value::String("rust".to_string()));
        connect.insert(
            "version".into(),
            Value::String(env!("CARGO_PKG_VERSION").to_string()),
        );
        connect.insert("protocol".into(), json!(1));
        connect.insert("headers".into(), Value::Bool(true));
        connect.insert("no_responders".into(), Value::Bool(true));
        if let Some(username) = &address.username {
            connect.insert("user".into(), Value::String(username.clone()));
        }
        if let Some(password) = &address.password {
            connect.insert("pass".into(), Value::String(password.clone()));
        }

        let mut writer = write_half;
        writer
            .write_all(Value::Object(connect).to_string().as_bytes())
            .await?;
        writer.write_all(b"\r\nPING\r\n").await?;
        writer.flush().await?;

        loop {
            let line = Self::read_protocol_line(&mut reader)
                .await?
                .ok_or_else(|| Self::protocol_error("NATS connection closed during handshake"))?;
            if line == "PONG" {
                break;
            }
            if line.starts_with("-ERR") {
                return Err(Self::protocol_error(line));
            }
        }

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
            ^ CONNECTION_NONCE.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            reader,
            writer,
            inbox_nonce: nonce,
            inbox_counter: 0,
            next_sid: 1,
        })
    }

    pub(crate) async fn subscribe(&mut self, subject: &str, sid: u64) -> BpResult<()> {
        NatsSubject::validate(subject)?;
        self.writer
            .write_all(format!("SUB {subject} {sid}\r\n").as_bytes())
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(crate) async fn unsubscribe(&mut self, sid: u64) -> BpResult<()> {
        self.writer
            .write_all(format!("UNSUB {sid}\r\n").as_bytes())
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(crate) async fn publish(&mut self, subject: &str, payload: &[u8]) -> BpResult<()> {
        self.publish_with_reply(subject, None, payload).await
    }

    pub(crate) async fn publish_with_reply(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
    ) -> BpResult<()> {
        NatsSubject::validate(subject)?;
        let header = match reply_to {
            Some(reply) => format!("PUB {subject} {reply} {}\r\n", payload.len()),
            None => format!("PUB {subject} {}\r\n", payload.len()),
        };
        self.writer.write_all(header.as_bytes()).await?;
        self.writer.write_all(payload).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(crate) async fn publish_with_headers(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        headers: &[(String, String)],
        payload: &[u8],
    ) -> BpResult<()> {
        NatsSubject::validate(subject)?;
        let block = Message::format_header_block(headers);
        let header = match reply_to {
            Some(reply) => format!(
                "HPUB {subject} {reply} {} {}\r\n",
                block.len(),
                block.len() + payload.len()
            ),
            None => format!(
                "HPUB {subject} {} {}\r\n",
                block.len(),
                block.len() + payload.len()
            ),
        };
        self.writer.write_all(header.as_bytes()).await?;
        self.writer.write_all(&block).await?;
        self.writer.write_all(payload).await?;
        self.writer.write_all(b"\r\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Performs a JetStream-style request: publish with an inbox reply subject
    /// and wait for a single response, mapping a 503 status to an error.
    pub(crate) async fn request(
        &mut self,
        subject: &str,
        payload: &[u8],
        response_timeout: Duration,
    ) -> BpResult<Message> {
        self.request_inner(subject, None, payload, response_timeout)
            .await
    }

    /// Request variant that publishes with headers, as required for JetStream
    /// publish acknowledgements carrying a deterministic message id.
    pub(crate) async fn request_with_headers(
        &mut self,
        subject: &str,
        headers: &[(String, String)],
        payload: &[u8],
        response_timeout: Duration,
    ) -> BpResult<Message> {
        self.request_inner(subject, Some(headers), payload, response_timeout)
            .await
    }

    async fn request_inner(
        &mut self,
        subject: &str,
        headers: Option<&[(String, String)]>,
        payload: &[u8],
        response_timeout: Duration,
    ) -> BpResult<Message> {
        NatsSubject::validate(subject)?;
        let sid = self.allocate_sid();
        let inbox = self.next_inbox();
        self.subscribe(&inbox, sid).await?;
        let result = match headers {
            Some(headers) => {
                self.publish_with_headers(subject, Some(&inbox), headers, payload)
                    .await
            }
            None => {
                self.publish_with_reply(subject, Some(&inbox), payload)
                    .await
            }
        };
        let result = match result {
            Ok(()) => {
                let deadline = Instant::now() + REQUEST_TIMEOUT + response_timeout;
                self.wait_for_response(sid, subject, deadline).await
            }
            Err(error) => Err(error),
        };
        let _ = self.unsubscribe(sid).await;
        result
    }

    async fn wait_for_response(
        &mut self,
        sid: u64,
        subject: &str,
        deadline: Instant,
    ) -> BpResult<Message> {
        loop {
            match timeout_at(deadline, self.next_frame()).await {
                Err(_) => {
                    return Err(Self::protocol_error(format!(
                        "timed out waiting for a response from {subject}"
                    )));
                }
                Ok(Err(error)) => return Err(error),
                Ok(Ok((frame_sid, message))) => {
                    if frame_sid != sid {
                        continue;
                    }
                    if message.is_status(503) {
                        return Err(Self::protocol_error(format!("no responders for {subject}")));
                    }
                    return Ok(message);
                }
            }
        }
    }

    /// Reads the next delivered message, responding to server pings. Returns
    /// the subscription identifier the message was delivered on.
    pub(crate) async fn next_frame(&mut self) -> BpResult<(u64, Message)> {
        loop {
            let Some(line) = Self::read_protocol_line(&mut self.reader).await? else {
                return Err(Self::protocol_error("NATS connection closed by server"));
            };
            if line == "PING" {
                self.writer.write_all(b"PONG\r\n").await?;
                self.writer.flush().await?;
                continue;
            }
            if line.starts_with("MSG ") || line.starts_with("HMSG ") {
                return self.read_message(&line).await;
            }
            if line.starts_with("-ERR") {
                return Err(Self::protocol_error(line));
            }
            if line.is_empty()
                || line.starts_with("+OK")
                || line.starts_with("INFO")
                || line == "PONG"
            {
                continue;
            }
            return Err(Self::protocol_error(format!(
                "unsupported NATS protocol message {line:?}"
            )));
        }
    }

    /// Reads the next delivered message payload, ignoring subscriptions other
    /// than the one this connection tracks.
    pub(crate) async fn next_message(&mut self) -> BpResult<Option<(String, Vec<u8>)>> {
        let (_, message) = self.next_frame().await?;
        Ok(Some((message.subject, message.payload)))
    }

    async fn read_message(&mut self, line: &str) -> BpResult<(u64, Message)> {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        match tokens.as_slice() {
            [op, subject, sid, size] if *op == "MSG" => {
                let size = Self::parse_size(size, line)?;
                let payload = self.read_exact_payload(size).await?;
                Ok((
                    Self::parse_sid(sid, line)?,
                    Message {
                        subject: (*subject).to_string(),
                        reply_to: None,
                        status: None,
                        headers: Vec::new(),
                        payload,
                    },
                ))
            }
            [op, subject, sid, reply_to, size] if *op == "MSG" => {
                let size = Self::parse_size(size, line)?;
                let payload = self.read_exact_payload(size).await?;
                Ok((
                    Self::parse_sid(sid, line)?,
                    Message {
                        subject: (*subject).to_string(),
                        reply_to: Some((*reply_to).to_string()),
                        status: None,
                        headers: Vec::new(),
                        payload,
                    },
                ))
            }
            [op, subject, sid, header_size, size] if *op == "HMSG" => {
                let header_size = Self::parse_size(header_size, line)?;
                let size = Self::parse_size(size, line)?;
                let (status, headers, payload) =
                    self.read_header_message(size, header_size).await?;
                Ok((
                    Self::parse_sid(sid, line)?,
                    Message {
                        subject: (*subject).to_string(),
                        reply_to: None,
                        status,
                        headers,
                        payload,
                    },
                ))
            }
            [op, subject, sid, reply_to, header_size, size] if *op == "HMSG" => {
                let header_size = Self::parse_size(header_size, line)?;
                let size = Self::parse_size(size, line)?;
                let (status, headers, payload) =
                    self.read_header_message(size, header_size).await?;
                Ok((
                    Self::parse_sid(sid, line)?,
                    Message {
                        subject: (*subject).to_string(),
                        reply_to: Some((*reply_to).to_string()),
                        status,
                        headers,
                        payload,
                    },
                ))
            }
            _ => Err(Self::protocol_error(format!(
                "malformed message line {line:?}"
            ))),
        }
    }

    async fn read_header_message(
        &mut self,
        size: usize,
        header_size: usize,
    ) -> BpResult<(Option<u16>, Headers, Vec<u8>)> {
        if header_size > size {
            return Err(Self::protocol_error(
                "NATS header size exceeds the message size",
            ));
        }
        let mut block = vec![0_u8; size];
        self.reader.read_exact(&mut block).await?;
        let mut terminator = [0_u8; 2];
        self.reader.read_exact(&mut terminator).await?;
        let payload = block.split_off(header_size);
        let (status, headers) =
            Message::parse_header_block(&block).map_err(Self::protocol_error)?;
        Ok((status, headers, payload))
    }

    async fn read_exact_payload(&mut self, size: usize) -> BpResult<Vec<u8>> {
        let mut payload = vec![0_u8; size];
        self.reader.read_exact(&mut payload).await?;
        let mut terminator = [0_u8; 2];
        self.reader.read_exact(&mut terminator).await?;
        Ok(payload)
    }

    pub(crate) fn next_inbox(&mut self) -> String {
        self.inbox_counter += 1;
        format!("_INBOX.{}.{}", self.inbox_nonce, self.inbox_counter)
    }

    pub(crate) fn allocate_sid(&mut self) -> u64 {
        let sid = self.next_sid;
        self.next_sid += 1;
        sid
    }

    fn parse_sid(sid: &str, line: &str) -> BpResult<u64> {
        sid.parse::<u64>()
            .map_err(|_| Self::protocol_error(format!("malformed subscription id in {line:?}")))
    }

    fn parse_size(size: &str, line: &str) -> BpResult<usize> {
        size.parse::<usize>()
            .map_err(|_| Self::protocol_error(format!("malformed message size in {line:?}")))
    }

    async fn read_protocol_line(reader: &mut BufReader<OwnedReadHalf>) -> BpResult<Option<String>> {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        Ok(Some(line.trim_end_matches(['\r', '\n']).to_string()))
    }

    pub(crate) fn protocol_error(message: impl Into<String>) -> BpError {
        BpError::Ingester(IngesterError::protocol(message))
    }
}
