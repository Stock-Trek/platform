use crate::streaming::stream_types::{Frame, GapDetail, GapReason, REPLAY_WINDOW};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};

pub enum SubscribeOutcome {
    Replay(Vec<Frame>),
    Gap(Frame),
    UnknownChannel,
}

struct ChannelState {
    sequence: u64,
    // TODO: stream frames from the shared log instead of keeping them in memory.
    frames: VecDeque<Frame>,
    full_replay: bool,
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

    pub fn publish(&self, channel: &str, frame: Frame) -> u64 {
        let mut channels = self.channels.lock();
        let state = channels
            .entry(channel.to_string())
            .or_insert_with(|| ChannelState {
                sequence: 0,
                frames: VecDeque::new(),
                full_replay: channel.starts_with("backtests."),
            });
        state.sequence += 1;
        let sequence = state.sequence;
        let mut frame = frame;
        frame.sequence = sequence;
        state.frames.push_back(frame.clone());
        if !state.full_replay && state.frames.len() > REPLAY_WINDOW {
            state.frames.pop_front();
        }
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
        let channels = self.channels.lock();
        let Some(state) = channels.get(channel) else {
            return SubscribeOutcome::UnknownChannel;
        };
        match last_sequence {
            None if state.full_replay => {
                SubscribeOutcome::Replay(state.frames.iter().cloned().collect())
            }
            None => SubscribeOutcome::Replay(Vec::new()),
            Some(last) => {
                let buffered = state.frames.front().map(|frame| frame.sequence);
                let Some(after) = buffered else {
                    return SubscribeOutcome::Replay(Vec::new());
                };
                if last < after - 1 || last > state.sequence {
                    return SubscribeOutcome::Gap(Frame::gap(
                        channel,
                        GapDetail {
                            after_sequence: last,
                            head_sequence: state.sequence,
                            reason: if last > state.sequence {
                                GapReason::AheadOfHead
                            } else {
                                GapReason::Expired
                            },
                        },
                    ));
                }
                SubscribeOutcome::Replay(
                    state
                        .frames
                        .iter()
                        .filter(|frame| frame.sequence > last)
                        .cloned()
                        .collect(),
                )
            }
        }
    }

    pub fn forget(&self, channel: &str) {
        self.channels.lock().remove(channel);
    }
}
