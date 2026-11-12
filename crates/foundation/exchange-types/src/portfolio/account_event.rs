use super::AccountPositionEvent;

#[derive(Debug, Clone, PartialEq)]
pub enum AccountEvent {
    Position(AccountPositionEvent),
    SessionExpired,
}
