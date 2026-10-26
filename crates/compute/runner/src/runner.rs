use crate::{
    command::Command,
    external::Enqueue,
    generated::dto::{PortfolioDto, StrategyContextDto},
};
use platform_exit_codes::ExitCode;
use prost::Message;
use std::collections::BTreeSet;
use stock_trek::{
    Algorithm, ResolvedContext, StrategyContext, prelude::traitreg, signals::SignalContext,
};

#[traitreg::registry(Algorithm)]
static ALGORITHM_REGISTRY: () = ();

pub struct Runner;

impl Runner {
    pub fn run(command: Command) -> i32 {
        let result =
            Self::select_algorithm(command.strategy_name()).and_then(|algorithm| match &command {
                Command::Algorithm { .. } => Self::cmd_algorithm(algorithm.as_ref()),
                Command::Execute {
                    strategy_context_path,
                    portfolio_path,
                    ..
                } => Self::cmd_execute(algorithm.as_ref(), strategy_context_path, portfolio_path),
                Command::Metadata { .. } => Self::cmd_metadata(algorithm.as_ref()),
                Command::Preferences { .. } => Self::cmd_preferences(algorithm.as_ref()),
                Command::Signals {
                    strategy_context_path,
                    ..
                } => Self::cmd_signals(algorithm.as_ref(), strategy_context_path),
            });
        match result {
            Ok(()) => ExitCode::Success.code(),
            Err(error) => {
                eprintln!("{error}");
                error.code()
            }
        }
    }

    fn select_algorithm(strategy_name: &str) -> Result<Box<dyn Algorithm>, ExitCode> {
        let mut names = BTreeSet::new();
        if ALGORITHM_REGISTRY
            .iter()
            .any(|registration| !names.insert(registration.name()))
        {
            return Err(ExitCode::StrategyNameNotUnique);
        }

        let mut registrations = ALGORITHM_REGISTRY.iter().filter(|registration| {
            registration.name() == strategy_name || registration.path() == strategy_name
        });
        let Some(registration) = registrations.next() else {
            if ALGORITHM_REGISTRY.iter().next().is_some()
                && ALGORITHM_REGISTRY
                    .iter()
                    .all(|registration| !registration.has_constructor())
            {
                return Err(ExitCode::StrategyHasNoConstructor);
            }
            return Err(ExitCode::StrategyNotFound);
        };
        if registrations.next().is_some() {
            return Err(ExitCode::StrategyNameNotUnique);
        }
        if !registration.has_constructor() {
            return Err(ExitCode::StrategyHasNoConstructor);
        }
        registration
            .instanciate()
            .ok_or(ExitCode::StrategyCouldNotInstantiate)
    }

    fn cmd_algorithm(algorithm: &dyn Algorithm) -> Result<(), ExitCode> {
        let command = algorithm.strategy(&StrategyContext::new());
        Enqueue::strategy(command).map_err(|_| ExitCode::ServerEntryFailedToEnqueueJson)
    }

    fn cmd_execute(
        algorithm: &dyn Algorithm,
        strategy_context_path: &str,
        portfolio_path: &str,
    ) -> Result<(), ExitCode> {
        Self::cmd_metadata(algorithm)?;
        let signal_context = Self::load_signal_context(strategy_context_path)?;
        let portfolio_data = std::fs::read(portfolio_path)
            .map_err(|_| ExitCode::ServerRunnerPortfolioFileNotFound)?;
        let portfolio_dto = PortfolioDto::decode(&portfolio_data[..])
            .map_err(|_| ExitCode::ServerRunnerPortfolioNotDecoded)?;
        let portfolio = portfolio_dto
            .try_into()
            .map_err(|_| ExitCode::ServerRunnerPortfolioNotCreated)?;
        let signals = algorithm.signals(&signal_context);
        let strategy_context = StrategyContext::new();
        let command = algorithm.strategy(&strategy_context);
        let mut resolved_context = ResolvedContext {
            enqueue_action: Box::new(Enqueue::action),
            portfolio,
            signals,
        };
        command
            .execute(&mut resolved_context)
            .map_err(|_| ExitCode::BotErrorResolvingAction)
    }

    fn cmd_metadata(algorithm: &dyn Algorithm) -> Result<(), ExitCode> {
        let name = algorithm.name();
        let description = algorithm.description();
        Enqueue::metadata(name, description).map_err(|_| ExitCode::ServerEntryFailedToEnqueueJson)
    }

    fn cmd_preferences(algorithm: &dyn Algorithm) -> Result<(), ExitCode> {
        let preferences = algorithm.preferences();
        Enqueue::preferences(preferences).map_err(|_| ExitCode::ServerEntryFailedToEnqueueJson)
    }

    fn cmd_signals(algorithm: &dyn Algorithm, strategy_context_path: &str) -> Result<(), ExitCode> {
        let signal_context = Self::load_signal_context(strategy_context_path)?;
        let signals = algorithm.signals(&signal_context);
        Enqueue::signals(signals).map_err(|_| ExitCode::ServerEntryFailedToEnqueueJson)
    }

    fn load_signal_context(strategy_context_path: &str) -> Result<SignalContext, ExitCode> {
        let strategy_context_data = std::fs::read(strategy_context_path)
            .map_err(|_| ExitCode::ServerRunnerStrategyContextFileNotFound)?;
        let strategy_context_dto = StrategyContextDto::decode(&strategy_context_data[..])
            .map_err(|_| ExitCode::ServerRunnerStrategyContextNotDecoded)?;
        strategy_context_dto
            .try_into()
            .map_err(|_| ExitCode::ServerRunnerStrategyContextNotCreated)
    }
}

pub fn run(command: Command) -> i32 {
    Runner::run(command)
}
