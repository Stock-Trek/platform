use serde::Deserialize;

/// A request to run the algorithm.
///
/// The pre-signed URL for the compiled runner artifact is provided alongside
/// the command. It is optional while native algorithm execution remains the
/// only supported invocation path.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRequest {
    #[serde(default)]
    pub cwasm_get_url: Option<String>,
    #[serde(flatten)]
    pub command: Command,
}

/// A request to run the algorithm, together with the inputs that command
/// needs. This is the single type that defines a build request: the JSON body
/// received on stdin deserialises straight into it.
///
/// The strategy is identified by name and is always required. Portfolio and
/// strategy context are referenced by file path; their contents are never
/// provided inline.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(
    tag = "command",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[non_exhaustive]
pub enum Command {
    Algorithm {
        strategy_name: String,
    },
    Execute {
        strategy_name: String,
        strategy_context_path: String,
        portfolio_path: String,
    },
    Metadata {
        strategy_name: String,
    },
    Preferences {
        strategy_name: String,
    },
    Signals {
        strategy_name: String,
        strategy_context_path: String,
    },
}

impl Command {
    pub fn strategy_name(&self) -> &str {
        match self {
            Command::Algorithm { strategy_name }
            | Command::Execute { strategy_name, .. }
            | Command::Metadata { strategy_name }
            | Command::Preferences { strategy_name }
            | Command::Signals { strategy_name, .. } => strategy_name,
        }
    }
}
