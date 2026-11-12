use platform_messages::SqsMessage;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventResponse {
    Accept(Vec<SqsMessage>),
    Duplicate,
    Ignore,
    Pong,
    Unauthorized,
    WrongGitProvider,
    Rejected,
}
