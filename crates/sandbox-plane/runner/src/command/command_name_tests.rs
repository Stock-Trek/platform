use super::*;

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn parses_each_command_name() {
    for (value, expected) in [
        ("Algorithm", CommandName::Algorithm),
        ("Execute", CommandName::Execute),
        ("Metadata", CommandName::Metadata),
        ("Preferences", CommandName::Preferences),
        ("Signals", CommandName::Signals),
    ] {
        assert_eq!(CommandName::parse(value), Some(expected));
    }
}

#[test]
fn rejects_unknown_command_names() {
    for value in ["", "algorithm", "BackTest", "ExecuteNow"] {
        assert_eq!(CommandName::parse(value), None);
    }
}

#[test]
fn reports_argument_counts_including_the_program_name() {
    assert_eq!(CommandName::Algorithm.argument_count(), 3);
    assert_eq!(CommandName::Execute.argument_count(), 5);
    assert_eq!(CommandName::Metadata.argument_count(), 3);
    assert_eq!(CommandName::Preferences.argument_count(), 3);
    assert_eq!(CommandName::Signals.argument_count(), 4);
}

#[test]
fn builds_commands_from_arguments() {
    assert_eq!(
        CommandName::Algorithm.command(&args(&["program", "Algorithm", "Strategy"])),
        Command::Algorithm {
            strategy_name: "Strategy".to_string(),
        }
    );
    assert_eq!(
        CommandName::Execute.command(&args(&[
            "program",
            "Execute",
            "Strategy",
            "context",
            "portfolio"
        ])),
        Command::Execute {
            strategy_name: "Strategy".to_string(),
            strategy_context_path: "context".to_string(),
            portfolio_path: "portfolio".to_string(),
        }
    );
    assert_eq!(
        CommandName::Metadata.command(&args(&["program", "Metadata", "Strategy"])),
        Command::Metadata {
            strategy_name: "Strategy".to_string(),
        }
    );
    assert_eq!(
        CommandName::Preferences.command(&args(&["program", "Preferences", "Strategy"])),
        Command::Preferences {
            strategy_name: "Strategy".to_string(),
        }
    );
    assert_eq!(
        CommandName::Signals.command(&args(&["program", "Signals", "Strategy", "context"])),
        Command::Signals {
            strategy_name: "Strategy".to_string(),
            strategy_context_path: "context".to_string(),
        }
    );
}
