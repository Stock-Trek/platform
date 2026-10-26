use serde::Deserialize;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialises_camel_case_command_payloads() {
        let command: Command = serde_json::from_str(
            r#"{
                "command": "execute",
                "strategyName": "AlgorithmStub",
                "strategyContextPath": "/tmp/strategy-context.bin",
                "portfolioPath": "/tmp/portfolio.bin"
            }"#,
        )
        .unwrap();

        assert_eq!(
            command,
            Command::Execute {
                strategy_name: "AlgorithmStub".to_string(),
                strategy_context_path: "/tmp/strategy-context.bin".to_string(),
                portfolio_path: "/tmp/portfolio.bin".to_string(),
            }
        );
    }
}
