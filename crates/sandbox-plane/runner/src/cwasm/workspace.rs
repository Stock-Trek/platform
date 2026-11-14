use crate::Command;
use platform_exit_codes::ExitCode;
use std::{fs, path::Path};

pub const STRATEGY_CONTEXT_FILE_NAME: &str = "strategy-context.bin";
pub const PORTFOLIO_FILE_NAME: &str = "portfolio.bin";

const GUEST_ROOT: &str = "/";
const PROGRAM_NAME: &str = "cwasm-invoker";

pub struct GuestWorkspace {
    _directory: tempfile::TempDir,
    arguments: Vec<String>,
}

impl GuestWorkspace {
    pub fn for_command(command: &Command) -> Result<Self, ExitCode> {
        let directory = Self::create_directory()?;
        let arguments = match command {
            Command::Algorithm { strategy_name } => {
                vec![
                    PROGRAM_NAME.to_string(),
                    "Algorithm".to_string(),
                    strategy_name.clone(),
                ]
            }
            Command::Execute {
                strategy_name,
                strategy_context_path,
                portfolio_path,
            } => {
                Self::copy_input(
                    strategy_context_path,
                    &directory,
                    STRATEGY_CONTEXT_FILE_NAME,
                    ExitCode::ServerRunnerStrategyContextFileNotFound,
                    ExitCode::ServerRunnerStrategyContextNotCreated,
                )?;
                Self::copy_input(
                    portfolio_path,
                    &directory,
                    PORTFOLIO_FILE_NAME,
                    ExitCode::ServerRunnerPortfolioFileNotFound,
                    ExitCode::ServerRunnerPortfolioNotCreated,
                )?;
                vec![
                    PROGRAM_NAME.to_string(),
                    "Execute".to_string(),
                    strategy_name.clone(),
                    Self::guest_path(STRATEGY_CONTEXT_FILE_NAME),
                    Self::guest_path(PORTFOLIO_FILE_NAME),
                ]
            }
            Command::Metadata { strategy_name } => {
                vec![
                    PROGRAM_NAME.to_string(),
                    "Metadata".to_string(),
                    strategy_name.clone(),
                ]
            }
            Command::Preferences { strategy_name } => {
                vec![
                    PROGRAM_NAME.to_string(),
                    "Preferences".to_string(),
                    strategy_name.clone(),
                ]
            }
            Command::Signals {
                strategy_name,
                strategy_context_path,
            } => {
                Self::copy_input(
                    strategy_context_path,
                    &directory,
                    STRATEGY_CONTEXT_FILE_NAME,
                    ExitCode::ServerRunnerStrategyContextFileNotFound,
                    ExitCode::ServerRunnerStrategyContextNotCreated,
                )?;
                vec![
                    PROGRAM_NAME.to_string(),
                    "Signals".to_string(),
                    strategy_name.clone(),
                    Self::guest_path(STRATEGY_CONTEXT_FILE_NAME),
                ]
            }
            _ => {
                eprintln!("cwasm-invoker: unsupported command");
                return Err(ExitCode::ServerEntryFailedToParseRequest);
            }
        };
        Ok(Self {
            _directory: directory,
            arguments,
        })
    }

    pub fn path(&self) -> &Path {
        self._directory.path()
    }

    pub fn arguments_as_str(&self) -> Vec<&str> {
        self.arguments.iter().map(String::as_str).collect()
    }

    fn create_directory() -> Result<tempfile::TempDir, ExitCode> {
        tempfile::Builder::new()
            .prefix("platform-guest-")
            .tempdir()
            .map_err(|error| {
                eprintln!("cwasm-invoker: failed to create guest workspace: {error}");
                ExitCode::BotErrorCreatingScratchPad
            })
    }

    fn copy_input(
        source: &str,
        directory: &tempfile::TempDir,
        name: &str,
        not_found: ExitCode,
        not_created: ExitCode,
    ) -> Result<(), ExitCode> {
        let source_path = Path::new(source);
        match fs::metadata(source_path) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => {
                eprintln!("cwasm-invoker: input {source:?} is not a regular file");
                return Err(not_found);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("cwasm-invoker: input {source:?} was not found");
                return Err(not_found);
            }
            Err(error) => {
                eprintln!("cwasm-invoker: failed to inspect input {source:?}: {error}");
                return Err(not_created);
            }
        }

        let destination = directory.path().join(name);
        fs::copy(source_path, &destination).map_err(|error| {
            eprintln!(
                "cwasm-invoker: failed to copy input {source:?} into the guest workspace: {error}"
            );
            not_created
        })?;
        Ok(())
    }

    fn guest_path(name: &str) -> String {
        format!("{GUEST_ROOT}{name}")
    }
}
