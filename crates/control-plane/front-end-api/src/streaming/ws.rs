use crate::auth::SessionService;
use crate::streaming::StreamService;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::{SinkExt, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_websockets::{CloseCode, Message, WebSocketStream};

const PING_INTERVAL: Duration = Duration::from_secs(30);
const OUTBOUND_BUFFER: usize = 256;
const WEBSOCKET_MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const KEY_LENGTH: usize = 24;
const MAX_MISSED_PONGS: u32 = 2;
const CLOSE_REVOKED: u16 = 4401;

pub(super) struct PongTracker {
    missed: u32,
}

impl PongTracker {
    pub(super) fn new() -> Self {
        Self { missed: 0 }
    }

    pub(super) fn on_ping(&mut self) -> bool {
        if self.missed >= MAX_MISSED_PONGS {
            return true;
        }
        self.missed += 1;
        false
    }

    pub(super) fn on_pong(&mut self) {
        self.missed = 0;
    }
}

pub struct Connection;

impl Connection {
    pub fn accept_key(key: &str) -> Option<String> {
        let key = key.trim();
        (key.len() == KEY_LENGTH && STANDARD.decode(key).is_ok()).then(|| {
            let digest = Sha1::digest(format!("{key}{WEBSOCKET_MAGIC}").as_bytes());
            STANDARD.encode(digest)
        })
    }

    pub async fn serve(
        ws: WebSocketStream<TokioIo<Upgraded>>,
        service: Arc<StreamService>,
        sessions: Arc<SessionService>,
        owner: String,
        session_id: String,
    ) {
        let connection_id = uuid::Uuid::new_v4().simple().to_string();
        service.connect(&connection_id, &owner);
        let (mut sink, mut source) = ws.split();
        let (outbound, mut outbound_rx) = mpsc::channel::<Message>(OUTBOUND_BUFFER);
        let mut forwarders = JoinSet::new();
        let mut pongs = PongTracker::new();
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await;
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    if sessions.revoked(&session_id) {
                        let _ = sink.send(Self::close_revoked()).await;
                        break;
                    }
                    if pongs.on_ping() {
                        break;
                    }
                    if sink.send(Message::ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
                message = outbound_rx.recv() => {
                    let Some(message) = message else { break };
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
                inbound = source.next() => {
                    let Some(Ok(message)) = inbound else { break };
                    if Self::settles(&mut pongs, &message) {
                        break;
                    }
                    if let Some(action) = Self::action(&message) {
                        Self::apply(
                            &service,
                            &owner,
                            &connection_id,
                            &outbound,
                            action,
                            &mut forwarders,
                        )
                        .await;
                    }
                }
            }
        }
        forwarders.abort_all();
        service.disconnect(&connection_id);
    }

    fn settles(pongs: &mut PongTracker, message: &Message) -> bool {
        if message.is_pong() {
            pongs.on_pong();
            return false;
        }
        message.is_close()
    }

    fn close_revoked() -> Message {
        let code = CloseCode::try_from(CLOSE_REVOKED).ok();
        Message::close(code, "")
    }

    fn action(message: &Message) -> Option<super::ws_channels::ClientAction> {
        serde_json::from_str(message.as_text()?).ok()
    }
}
