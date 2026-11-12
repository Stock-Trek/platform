use platform_exit_codes::ExitCode;
use platform_runner::{
    Command, SimulationParameters,
    dto::{PortfolioDto, StrategyContextDto},
    string::JsonString,
};
use prost::Message;
use std::{collections::BTreeMap, fs};
use stock_trek::{errors::StockTrekError, markets::AlignedWindow, types::AssetId};

use crate::cwasm::backtest::{Balances, market_data::Series, slots::MarketSlot};

pub(super) struct Spec {
    pub(super) strategy_name: String,
    pub(super) strategy_context: StrategyContextDto,
    pub(super) initial_balances: Balances,
    pub(super) market_slots: Vec<MarketSlot>,
    pub(super) driving_window_number: u32,
    pub(super) windows: Vec<(u32, AlignedWindow)>,
    pub(super) historical_candle_count: usize,
    pub(super) open_candle_progress_millis: u64,
    pub(super) simulation_count: u32,
    pub(super) max_concurrent_simulations: u32,
    pub(super) random_seed: u64,
    pub(super) simulation: SimulationParameters,
    pub(super) user_id: String,
    pub(super) algorithm_id: String,
    pub(super) back_test_id: String,
    pub(super) results_stream_url: String,
    pub(super) results_post_url: String,
    pub(super) steps: usize,
}

impl Spec {
    pub(super) fn new(command: &Command) -> Result<Self, i32> {
        let Command::BackTest {
            strategy_name,
            strategy_context_path,
            portfolio_path,
            market_data_path,
            results_stream_url,
            results_post_url,
            user_id,
            algorithm_id,
            back_test_id,
            simulation_count,
            max_concurrent_simulations,
            random_seed,
            trading_pairs,
            windows,
            historical_candle_count,
            open_candle_progress_millis,
            simulation,
        } = command
        else {
            return Err(ExitCode::ServerEntryFailedToParseRequest.code());
        };
        if *simulation_count < 1 {
            return Err(Self::invalid_argument(
                "simulation count must be at least one",
            ));
        }
        if *max_concurrent_simulations < 1 {
            return Err(Self::invalid_argument(
                "max concurrent simulations must be at least one",
            ));
        }
        for (name, chance) in [
            ("candle loss chance", simulation.candle_loss_chance),
            ("slippage chance", simulation.slippage_chance),
            ("lost order chance", simulation.lost_order_chance),
        ] {
            if !(0.0..=1.0).contains(&chance) {
                return Err(Self::invalid_argument(&format!(
                    "{name} must be between zero and one"
                )));
            }
        }
        if !(0.0..=1.0).contains(&simulation.slippage_proportion) {
            return Err(Self::invalid_argument(
                "slippage proportion must be between zero and one",
            ));
        }
        let strategy_context: StrategyContextDto = Self::read_dto(
            strategy_context_path,
            ExitCode::ServerRunnerStrategyContextFileNotFound,
            ExitCode::ServerRunnerStrategyContextNotDecoded,
        )?;
        let portfolio: PortfolioDto = Self::read_dto(
            portfolio_path,
            ExitCode::ServerRunnerPortfolioFileNotFound,
            ExitCode::ServerRunnerPortfolioNotDecoded,
        )?;
        let portfolio: stock_trek::Portfolio =
            portfolio.try_into().map_err(|error: StockTrekError| {
                eprintln!("cwasm-invoker: portfolio could not be created: {error}");
                ExitCode::ServerRunnerPortfolioNotCreated.code()
            })?;
        let mut initial_balances: Balances = BTreeMap::new();
        for (cex, account, asset, total) in portfolio.assets_iter() {
            initial_balances
                .entry(*cex)
                .or_default()
                .entry(account.clone())
                .or_default()
                .insert(*asset, *total);
        }
        let market_data: StrategyContextDto = Self::read_dto(
            market_data_path,
            ExitCode::ServerRunnerMarketDataFileNotFound,
            ExitCode::ServerRunnerMarketDataNotDecoded,
        )?;
        let mut pairs = Vec::new();
        for pair in trading_pairs {
            let base: AssetId = JsonString::deserialize(&pair.base).map_err(|error| {
                Self::invalid_argument(&format!("trading pair base is invalid: {error}"))
            })?;
            let quote: AssetId = JsonString::deserialize(&pair.quote).map_err(|error| {
                Self::invalid_argument(&format!("trading pair quote is invalid: {error}"))
            })?;
            pairs.push((base, quote));
        }
        pairs.sort();
        pairs.dedup();
        let mut parsed_windows = Vec::new();
        for number in windows.iter().copied() {
            let repr = u8::try_from(number)
                .map_err(|_| Self::invalid_argument(&format!("window {number} is invalid")))?;
            let window = AlignedWindow::from_repr(repr)
                .ok_or_else(|| Self::invalid_argument(&format!("window {number} is invalid")))?;
            parsed_windows.push((number, window));
        }
        parsed_windows.sort();
        parsed_windows.dedup();
        if parsed_windows.is_empty() {
            return Err(Self::invalid_argument("at least one window is required"));
        }
        if pairs.is_empty() {
            return Err(Self::invalid_argument(
                "at least one trading pair is required",
            ));
        }
        let window_numbers = parsed_windows
            .iter()
            .map(|(number, _)| *number)
            .collect::<Vec<_>>();
        let data = Series::load(&market_data, &pairs, &window_numbers)?;
        let driving_window = *parsed_windows
            .iter()
            .map(|(_, window)| window)
            .min()
            .ok_or(ExitCode::ServerEntryInvalidArgument.code())?;
        let market_slots = MarketSlot::build_all(&strategy_context, &data, &pairs)?;
        let driving_number = parsed_windows
            .iter()
            .find(|(_, window)| *window == driving_window)
            .map(|(number, _)| *number)
            .unwrap_or_default();

        let steps = market_slots
            .iter()
            .map(|slot| slot.series[&driving_number].candles.len())
            .min()
            .unwrap_or_default();
        if steps == 0 {
            return Err(Self::invalid_argument(
                "market data has no simulation steps",
            ));
        }
        let driving_millis = driving_window.millis().upper_bound();
        if *open_candle_progress_millis >= driving_millis {
            return Err(Self::invalid_argument(
                "open candle progress must be smaller than the shortest window",
            ));
        }
        Ok(Self {
            strategy_name: strategy_name.clone(),
            strategy_context,
            initial_balances,
            market_slots,
            driving_window_number: driving_number,
            windows: parsed_windows,
            historical_candle_count: *historical_candle_count as usize,
            open_candle_progress_millis: *open_candle_progress_millis,
            simulation_count: *simulation_count,
            max_concurrent_simulations: *max_concurrent_simulations,
            random_seed: *random_seed,
            simulation: simulation.clone(),
            user_id: user_id.clone(),
            algorithm_id: algorithm_id.clone(),
            back_test_id: back_test_id.clone(),
            results_stream_url: results_stream_url.clone(),
            results_post_url: results_post_url.clone(),
            steps,
        })
    }

    fn read_dto<T: Message + Default>(
        path: &str,
        not_found: ExitCode,
        not_decoded: ExitCode,
    ) -> Result<T, i32> {
        let bytes = fs::read(path).map_err(|error| {
            eprintln!("cwasm-invoker: failed to read {path:?}: {error}");
            if error.kind() == std::io::ErrorKind::NotFound {
                not_found.code()
            } else {
                not_decoded.code()
            }
        })?;
        T::decode(&bytes[..]).map_err(|error| {
            eprintln!("cwasm-invoker: failed to decode {path:?}: {error}");
            not_decoded.code()
        })
    }

    pub(super) fn invalid_argument(message: &str) -> i32 {
        eprintln!("cwasm-invoker: {message}");
        ExitCode::ServerEntryInvalidArgument.code()
    }

    pub(super) fn not_decoded(message: &str) -> i32 {
        eprintln!("cwasm-invoker: {message}");
        ExitCode::ServerRunnerMarketDataNotDecoded.code()
    }
}
