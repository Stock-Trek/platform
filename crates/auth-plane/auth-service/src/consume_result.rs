use crate::replay_reservation::ReplayReservation;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumeResult {
    Consumed(ReplayReservation),
    Replayed,
    Expired,
}
