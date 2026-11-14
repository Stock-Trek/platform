use serde::Deserialize;

/// A request to run the algorithm.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRequest {
    pub cwasm_get_url: String,
    pub cwasm_sha256: String,
    pub cwasm_size: u64,
    pub wasmtime_version: String,
    pub order_request_url: String,
    pub auth_service_url: String,
    pub owner: String,
    #[serde(flatten)]
    pub command: Command,
}

/// A command to run the algorithm, together with the inputs that command
/// needs. The JSON body received on stdin deserialises into a [`RunRequest`],
/// which flattens this command into its fields.
///
/// The strategy is identified by name and is always required. Portfolio and
/// strategy context are referenced by file path; their contents are never
/// provided inline.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    tag = "command",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
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
    BackTest {
        strategy_name: String,
        strategy_context_path: String,
        portfolio_path: String,
        market_data_path: String,
        results_stream_url: String,
        results_post_url: String,
        user_id: String,
        algorithm_id: String,
        back_test_id: String,
        simulation_count: u32,
        max_concurrent_simulations: u32,
        random_seed: u64,
        trading_pairs: Vec<TradingPair>,
        windows: Vec<u32>,
        historical_candle_count: u32,
        open_candle_progress_millis: u64,
        #[serde(default)]
        simulation: SimulationParameters,
    },
}

/// A base and quote asset that the simulation trades against.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradingPair {
    pub base: String,
    pub quote: String,
}

/// The stochastic knobs of a back test simulation. Chances are probabilities
/// in the inclusive range zero to one and are evaluated once per candidate
/// candle, per order, and per fill respectively. The slippage proportion is
/// applied to the fill price in the adverse direction.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SimulationParameters {
    pub candle_loss_chance: f64,
    pub slippage_chance: f64,
    pub slippage_proportion: f64,
    pub lost_order_chance: f64,
}

impl Command {
    pub fn strategy_name(&self) -> &str {
        match self {
            Command::Algorithm { strategy_name }
            | Command::BackTest { strategy_name, .. }
            | Command::Execute { strategy_name, .. }
            | Command::Metadata { strategy_name }
            | Command::Preferences { strategy_name }
            | Command::Signals { strategy_name, .. } => strategy_name,
        }
    }

    pub fn arguments(&self) -> Vec<&str> {
        match self {
            Command::Algorithm { strategy_name } => vec!["Algorithm", strategy_name],
            Command::BackTest { strategy_name, .. } => vec!["BackTest", strategy_name],
            Command::Execute {
                strategy_name,
                strategy_context_path,
                portfolio_path,
            } => vec![
                "Execute",
                strategy_name,
                strategy_context_path,
                portfolio_path,
            ],
            Command::Metadata { strategy_name } => vec!["Metadata", strategy_name],
            Command::Preferences { strategy_name } => vec!["Preferences", strategy_name],
            Command::Signals {
                strategy_name,
                strategy_context_path,
            } => vec!["Signals", strategy_name, strategy_context_path],
        }
    }
}
