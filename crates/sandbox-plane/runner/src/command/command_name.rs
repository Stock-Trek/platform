use crate::command::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandName {
    Algorithm,
    Execute,
    Metadata,
    Preferences,
    Signals,
}

impl CommandName {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Algorithm" => Some(Self::Algorithm),
            "Execute" => Some(Self::Execute),
            "Metadata" => Some(Self::Metadata),
            "Preferences" => Some(Self::Preferences),
            "Signals" => Some(Self::Signals),
            _ => None,
        }
    }

    pub fn argument_count(self) -> usize {
        match self {
            Self::Algorithm | Self::Metadata | Self::Preferences => 3,
            Self::Execute => 5,
            Self::Signals => 4,
        }
    }

    pub fn command(self, args: &[String]) -> Command {
        match self {
            Self::Algorithm => Command::Algorithm {
                strategy_name: args[2].clone(),
            },
            Self::Execute => Command::Execute {
                strategy_name: args[2].clone(),
                strategy_context_path: args[3].clone(),
                portfolio_path: args[4].clone(),
            },
            Self::Metadata => Command::Metadata {
                strategy_name: args[2].clone(),
            },
            Self::Preferences => Command::Preferences {
                strategy_name: args[2].clone(),
            },
            Self::Signals => Command::Signals {
                strategy_name: args[2].clone(),
                strategy_context_path: args[3].clone(),
            },
        }
    }
}
