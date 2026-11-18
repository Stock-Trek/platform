use crate::streaming::stream_types::{Frame, GapDetail, GapReason, REPLAY_WINDOW};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use tokio::sync::broadcast;

const LIVE_BUFFER: usize = 1024;

pub enum SubscribeOutcome {
    Joined {
        replay: Vec<Frame>,
        live: broadcast::Receiver<Frame>,
    },
    Gap(Frame),
    UnknownChannel,
}

struct ChannelState {
    sequence: u64,
    frames: VecDeque<Frame>,
    full_replay: bool,
    live: broadcast::Sender<Frame>,
}

pub struct ChannelRegistry {
    channels: Mutex<HashMap<String, ChannelState>>,
}

impl ChannelRegistry {
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    #[allow(dead_code)]
    pub fn publish(&self, channel: &str, frame: Frame) -> u64 {
        let mut channels = self.channels.lock();
        let state = channels
            .entry(channel.to_string())
            .or_insert_with(|| ChannelState {
                sequence: 0,
                frames: VecDeque::new(),
                full_replay: channel.starts_with("backtests."),
                live: broadcast::channel(LIVE_BUFFER).0,
            });
        state.sequence += 1;
        let sequence = state.sequence;
        let mut frame = frame;
        frame.sequence = sequence;
        state.frames.push_back(frame.clone());
        if !state.full_replay && state.frames.len() > REPLAY_WINDOW {
            state.frames.pop_front();
        }
        let _ = state.live.send(frame.clone());
        sequence
    }

    pub fn sequence(&self, channel: &str) -> u64 {
        self.channels
            .lock()
            .get(channel)
            .map(|state| state.sequence)
            .unwrap_or(0)
    }

    pub fn subscribe(&self, channel: &str, last_sequence: Option<u64>) -> SubscribeOutcome {
        let mut channels = self.channels.lock();
        let Some(state) = channels.get_mut(channel) else {
            return SubscribeOutcome::UnknownChannel;
        };
        match Self::replay(state, channel, last_sequence) {
            Ok(replay) => SubscribeOutcome::Joined {
                replay,
                live: state.live.subscribe(),
            },
            Err(gap) => SubscribeOutcome::Gap(gap),
        }
    }

    fn replay(
        state: &mut ChannelState,
        channel: &str,
        last_sequence: Option<u64>,
    ) -> Result<Vec<Frame>, Frame> {
        match last_sequence {
            None if state.full_replay => Ok(state.frames.iter().cloned().collect()),
            None => Ok(Vec::new()),
            Some(last) => {
                let after = state
                    .frames
                    .front()
                    .map(|frame| frame.sequence)
                    .unwrap_or(state.sequence + 1);
                let behind_head = state.sequence.saturating_sub(last);
                if last > state.sequence {
                    return Err(Frame::gap(
                        channel,
                        GapDetail {
                            after_sequence: last,
                            head_sequence: state.sequence,
                            reason: GapReason::AheadOfHead,
                        },
                    ));
                }
                if !state.full_replay && (behind_head > REPLAY_WINDOW as u64 || last + 1 < after) {
                    return Err(Frame::gap(
                        channel,
                        GapDetail {
                            after_sequence: last,
                            head_sequence: state.sequence,
                            reason: GapReason::Expired,
                        },
                    ));
                }
                Ok(state
                    .frames
                    .iter()
                    .filter(|frame| frame.sequence > last)
                    .cloned()
                    .collect())
            }
        }
    }

    #[allow(dead_code)]
    pub fn forget(&self, channel: &str) {
        self.channels.lock().remove(channel);
    }
}
