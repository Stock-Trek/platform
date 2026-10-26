#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::FromRepr)]
pub enum ExitCode {
    Success = 0,

    // strategy errors
    StrategyNotFound = 1,
    StrategyHasNoConstructor = 2,
    StrategyCouldNotInstantiate = 3,
    StrategyNameNotUnique = 4,

    // server errors
    ServerEntryIncorrectArgCount = 101,
    ServerEntryFailedToParseCommand = 102,
    ServerEntryFailedToConvertToJson = 103,
    ServerEntryFailedToSetLinkedFunction = 104,
    ServerEntryFailedToFindLinkedFunction = 105,
    ServerEntryFailedToEnqueueJson = 106,
    ServerEntryFailedToParseRequest = 107,
    ServerEntryInvalidArgument = 108,

    ServerRunnerStrategyContextFileNotFound = 111,
    ServerRunnerStrategyContextNotDecoded = 112,
    ServerRunnerStrategyContextNotCreated = 113,

    ServerRunnerPortfolioFileNotFound = 121,
    ServerRunnerPortfolioNotDecoded = 122,
    ServerRunnerPortfolioNotCreated = 123,

    ServerRunnerConnectionFailed = 131,

    // bot errors
    BotErrorCreatingResolver = 201,
    BotErrorCreatingScratchPad = 202,
    BotErrorResolvingAction = 203,
    BotErrorPanic = 204,
}

impl ExitCode {
    pub const fn code(self) -> i32 {
        self as i32
    }

    pub fn from_code(code: i32) -> Option<Self> {
        u8::try_from(code).ok().and_then(Self::from_repr)
    }

    pub const fn is_client_error(self) -> bool {
        matches!(
            self,
            Self::ServerEntryFailedToParseCommand
                | Self::ServerEntryFailedToParseRequest
                | Self::ServerEntryInvalidArgument
                | Self::StrategyNotFound
                | Self::StrategyHasNoConstructor
                | Self::StrategyCouldNotInstantiate
                | Self::StrategyNameNotUnique
                | Self::ServerRunnerStrategyContextFileNotFound
                | Self::ServerRunnerStrategyContextNotDecoded
                | Self::ServerRunnerStrategyContextNotCreated
                | Self::ServerRunnerPortfolioFileNotFound
                | Self::ServerRunnerPortfolioNotDecoded
                | Self::ServerRunnerPortfolioNotCreated
        )
    }

    pub fn is_client_error_code(code: i32) -> bool {
        Self::from_code(code).is_some_and(Self::is_client_error)
    }

    pub fn is_invalid_request_code(code: i32) -> bool {
        Self::is_client_error_code(code)
    }

    pub const fn message(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::StrategyNotFound => "strategy not found",
            Self::StrategyHasNoConstructor => "strategy has no constructor",
            Self::StrategyCouldNotInstantiate => "strategy could not be instantiated",
            Self::StrategyNameNotUnique => "strategy name is not unique",
            Self::ServerEntryIncorrectArgCount => "incorrect argument count",
            Self::ServerEntryFailedToParseCommand => "invalid request",
            Self::ServerEntryFailedToConvertToJson => "failed to convert response to JSON",
            Self::ServerEntryFailedToSetLinkedFunction => "failed to set linked function",
            Self::ServerEntryFailedToFindLinkedFunction => "failed to find linked function",
            Self::ServerEntryFailedToEnqueueJson => "failed to enqueue JSON",
            Self::ServerEntryFailedToParseRequest => "invalid request",
            Self::ServerEntryInvalidArgument => "invalid request",
            Self::ServerRunnerStrategyContextFileNotFound => "strategy context not found",
            Self::ServerRunnerStrategyContextNotDecoded => "strategy context could not be decoded",
            Self::ServerRunnerStrategyContextNotCreated => "strategy context could not be created",
            Self::ServerRunnerPortfolioFileNotFound => "portfolio not found",
            Self::ServerRunnerPortfolioNotDecoded => "portfolio could not be decoded",
            Self::ServerRunnerPortfolioNotCreated => "portfolio could not be created",
            Self::ServerRunnerConnectionFailed => "failed to connect to build runner",
            Self::BotErrorCreatingResolver => "failed to create resolver",
            Self::BotErrorCreatingScratchPad => "failed to create scratch pad",
            Self::BotErrorResolvingAction => "failed to resolve action",
            Self::BotErrorPanic => "build runner panicked",
        }
    }
}

impl From<ExitCode> for i32 {
    fn from(code: ExitCode) -> Self {
        code.code()
    }
}

impl std::fmt::Display for ExitCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for ExitCode {}

pub fn is_client_error(code: i32) -> bool {
    ExitCode::is_client_error_code(code)
}

pub fn is_invalid_request(code: i32) -> bool {
    ExitCode::is_invalid_request_code(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_code_returns_known_variants() {
        assert_eq!(ExitCode::from_code(0), Some(ExitCode::Success));
        assert_eq!(
            ExitCode::from_code(108),
            Some(ExitCode::ServerEntryInvalidArgument)
        );
        assert_eq!(ExitCode::from_code(204), Some(ExitCode::BotErrorPanic));
    }

    #[test]
    fn from_code_rejects_unknown_and_out_of_range_values() {
        assert_eq!(ExitCode::from_code(5), None);
        assert_eq!(ExitCode::from_code(-1), None);
        assert_eq!(ExitCode::from_code(i32::from(u8::MAX) + 1), None);
    }
}
