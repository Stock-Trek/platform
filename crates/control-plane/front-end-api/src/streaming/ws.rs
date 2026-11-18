use crate::streaming::StreamService;
use crate::streaming::channel_registry::SubscribeOutcome;
use crate::streaming::stream_types::{Frame, GapDetail, GapReason};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::{SinkExt, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use sha1::{Digest, Sha1};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_websockets::{Message, WebSocketStream};

const PING_INTERVAL: Duration = Duration::from_secs(30);
const OUTBOUND_BUFFER: usize = 256;
const WEBSOCKET_MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const KEY_LENGTH: usize = 24;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientAction {
    action: String,
    channel: String,
    #[serde(default)]
    last_sequence: Option<u64>,
}

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
    owner: String,
) {
    let connection_id = uuid::Uuid::new_v4().simple().to_string();
    service.connect(&connection_id, &owner);
    let (mut sink, mut source) = ws.split();
    let (outbound, mut outbound_rx) = mpsc::channel::<Message>(OUTBOUND_BUFFER);
    let mut forwarders = JoinSet::new();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ping.tick() => {
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
                let Some(action) = action(&message) else { break };
                apply(&service, &owner, &connection_id, &outbound, action, &mut forwarders)
                    .await;
            }
        }
    }
    forwarders.abort_all();
    service.disconnect(&connection_id);
}

fn action(message: &Message) -> Option<ClientAction> {
    serde_json::from_str(message.as_text()?).ok()
}

async fn apply(
    service: &Arc<StreamService>,
    owner: &str,
    connection_id: &str,
    outbound: &mpsc::Sender<Message>,
    action: ClientAction,
    forwarders: &mut JoinSet<()>,
) {
    match action.action.as_str() {
        "unsubscribe" => service.unsubscribe(connection_id, &action.channel),
        "subscribe" => subscribe(service, owner, connection_id, outbound, action, forwarders).await,
        _ => {
            let frame = Frame::error(&action.channel, "unsupported_action");
            let _ = outbound.send(Message::text(to_json(&frame))).await;
        }
    }
}

async fn subscribe(
    service: &Arc<StreamService>,
    owner: &str,
    connection_id: &str,
    outbound: &mpsc::Sender<Message>,
    action: ClientAction,
    forwarders: &mut JoinSet<()>,
) {
    let channel = action.channel.clone();
    if !service.owns_channel(owner, &channel) {
        let _ = outbound
            .send(Message::text(to_json(&Frame::error(&channel, "not_found"))))
            .await;
        return;
    }
    match service.subscribe(connection_id, &channel, action.last_sequence) {
        SubscribeOutcome::Joined { replay, live } => {
            let replay = replay.iter().map(to_json).collect();
            forwarders.spawn(forward(
                Arc::clone(service),
                channel,
                replay,
                live,
                outbound.clone(),
            ));
        }
        SubscribeOutcome::Gap(frame) => {
            let _ = outbound.send(Message::text(to_json(&frame))).await;
        }
        SubscribeOutcome::UnknownChannel => {
            let _ = outbound
                .send(Message::text(to_json(&Frame::error(&channel, "not_found"))))
                .await;
        }
    }
}

async fn forward(
    service: Arc<StreamService>,
    channel: String,
    replay: Vec<String>,
    mut live: broadcast::Receiver<Frame>,
    outbound: mpsc::Sender<Message>,
) {
    for frame in replay {
        if outbound.send(Message::text(frame)).await.is_err() {
            return;
        }
    }
    let mut last = 0;
    loop {
        match live.recv().await {
            Ok(frame) => {
                last = frame.sequence;
                if outbound.send(Message::text(to_json(&frame))).await.is_err() {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let gap = Frame::gap(
                    &channel,
                    GapDetail {
                        after_sequence: last,
                        head_sequence: service.registry().sequence(&channel),
                        reason: GapReason::Expired,
                    },
                );
                if outbound.send(Message::text(to_json(&gap))).await.is_err() {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

fn to_json(frame: &Frame) -> String {
    serde_json::to_string(frame).expect("frame json")
}
