use crate::streaming::StreamService;
use crate::streaming::stream_service::Subscription;
use crate::streaming::stream_types::{Frame, GapDetail, GapReason};
use crate::streaming::ws::Connection;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_websockets::Message;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClientAction {
    action: String,
    channel: String,
    #[serde(default)]
    last_sequence: Option<u64>,
}

impl Connection {
    pub(super) async fn apply(
        service: &Arc<StreamService>,
        owner: &str,
        connection_id: &str,
        outbound: &mpsc::Sender<Message>,
        action: ClientAction,
        forwarders: &mut JoinSet<()>,
    ) {
        match action.action.as_str() {
            "unsubscribe" => service.unsubscribe(connection_id, &action.channel),
            "subscribe" => {
                Self::subscribe(service, owner, connection_id, outbound, action, forwarders).await
            }
            _ => {
                let frame = Frame::error(&action.channel, "unsupported_action");
                let _ = outbound.send(Message::text(Self::to_json(&frame))).await;
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
                .send(Message::text(Self::to_json(&Frame::error(
                    &channel,
                    "not_found",
                ))))
                .await;
            return;
        }
        match service.subscribe(connection_id, &channel, action.last_sequence) {
            Subscription::Joined {
                replay,
                live,
                epoch,
            } => {
                let replay = replay.iter().map(Self::to_json).collect();
                forwarders.spawn(Self::forward(
                    Arc::clone(service),
                    connection_id.to_string(),
                    channel,
                    epoch,
                    replay,
                    live,
                    outbound.clone(),
                ));
            }
            Subscription::Gap(frame) => {
                let _ = outbound.send(Message::text(Self::to_json(&frame))).await;
            }
            Subscription::UnknownChannel => {
                let _ = outbound
                    .send(Message::text(Self::to_json(&Frame::error(
                        &channel,
                        "not_found",
                    ))))
                    .await;
            }
        }
    }

    pub(super) async fn forward(
        service: Arc<StreamService>,
        connection_id: String,
        channel: String,
        epoch: u64,
        replay: Vec<String>,
        mut live: broadcast::Receiver<Frame>,
        outbound: mpsc::Sender<Message>,
    ) {
        for frame in replay {
            if !Self::current(&service, &connection_id, &channel, epoch)
                || outbound.send(Message::text(frame)).await.is_err()
            {
                return;
            }
        }
        let mut last = 0;
        loop {
            match live.recv().await {
                Ok(frame) => {
                    last = frame.sequence;
                    if !Self::current(&service, &connection_id, &channel, epoch)
                        || outbound
                            .send(Message::text(Self::to_json(&frame)))
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if !Self::current(&service, &connection_id, &channel, epoch) {
                        return;
                    }
                    let gap = Frame::gap(
                        &channel,
                        GapDetail {
                            after_sequence: last,
                            head_sequence: service.registry().sequence(&channel),
                            reason: GapReason::Expired,
                        },
                    );
                    if outbound
                        .send(Message::text(Self::to_json(&gap)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }

    fn current(service: &StreamService, connection_id: &str, channel: &str, epoch: u64) -> bool {
        service.subscription_epoch(connection_id, channel) == Some(epoch)
    }

    pub(super) fn to_json(frame: &Frame) -> String {
        serde_json::to_string(frame).expect("frame json")
    }
}
