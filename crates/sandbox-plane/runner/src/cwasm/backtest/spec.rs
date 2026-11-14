use crate::{
    Command, SimulationParameters,
    dto::{PortfolioDto, StrategyContextDto},
    string::JsonString,
};
use prost::Message;
use std::{collections::BTreeMap, fs, path::PathBuf};
use stock_trek::{markets::AlignedWindow, types::AssetId};

use crate::cwasm::backtest::{
    BacktestError, Balances, Input, market_data::Series, slots::MarketSlot,
};

pub struct Spec {
    pub strategy_name: String,
    pub strategy_context: StrategyContextDto,
    pub initial_balances: Balances,
    pub market_slots: Vec<MarketSlot>,
    pub driving_window_number: u32,
    pub windows: Vec<(u32, AlignedWindow)>,
    pub historical_candle_count: usize,
    pub open_candle_progress_millis: u64,
    pub simulation_count: u32,
    pub max_concurrent_simulations: u32,
    pub random_seed: u64,
    pub simulation: SimulationParameters,
    pub user_id: String,
    pub algorithm_id: String,
    pub back_test_id: String,
    pub results_stream_url: String,
    pub results_post_url: String,
    pub steps: usize,
}

impl Spec {
    pub fn new(command: &Command) -> Result<Self, BacktestError> {
        let Command::Backtest {
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
            return Err(BacktestError::UnsupportedCommand);
        };
        if *simulation_count < 1 {
            return Err(BacktestError::invalid_argument(
                "simulation count must be at least one",
            ));
        }
        if *max_concurrent_simulations < 1 {
            return Err(BacktestError::invalid_argument(
                "max concurrent simulations must be at least one",
            ));
        }
        for (name, chance) in [
            ("candle loss chance", simulation.candle_loss_chance),
            ("slippage chance", simulation.slippage_chance),
            ("lost order chance", simulation.lost_order_chance),
        ] {
            if !(0.0..=1.0).contains(&chance) {
                return Err(BacktestError::invalid_argument(format!(
                    "{name} must be between zero and one"
                )));
            }
        }
        if !(0.0..=1.0).contains(&simulation.slippage_proportion) {
            return Err(BacktestError::invalid_argument(
                "slippage proportion must be between zero and one",
            ));
        }
        let strategy_context: StrategyContextDto =
            Self::read_dto(strategy_context_path, Input::StrategyContext)?;
        let portfolio: PortfolioDto = Self::read_dto(portfolio_path, Input::Portfolio)?;
        let portfolio: stock_trek::Portfolio = portfolio
            .try_into()
            .map_err(BacktestError::InvalidPortfolio)?;
        let mut initial_balances: Balances = BTreeMap::new();
        for (cex, account, asset, total) in portfolio.assets_iter() {
            initial_balances
                .entry(*cex)
                .or_default()
                .entry(account.clone())
                .or_default()
                .insert(*asset, *total);
        }
        let market_data: StrategyContextDto = Self::read_dto(market_data_path, Input::MarketData)?;
        let mut pairs = Vec::new();
        for pair in trading_pairs {
            let base: AssetId = JsonString::deserialize(&pair.base).map_err(|error| {
                BacktestError::invalid_argument(format!("trading pair base is invalid: {error}"))
            })?;
            let quote: AssetId = JsonString::deserialize(&pair.quote).map_err(|error| {
                BacktestError::invalid_argument(format!("trading pair quote is invalid: {error}"))
            })?;
            pairs.push((base, quote));
        }
        pairs.sort();
        pairs.dedup();
        let mut parsed_windows = Vec::new();
        for number in windows.iter().copied() {
            let repr = u8::try_from(number).map_err(|_| {
                BacktestError::invalid_argument(format!("window {number} is invalid"))
            })?;
            let window = AlignedWindow::from_repr(repr).ok_or_else(|| {
                BacktestError::invalid_argument(format!("window {number} is invalid"))
            })?;
            parsed_windows.push((number, window));
        }
        parsed_windows.sort();
        parsed_windows.dedup();
        if parsed_windows.is_empty() {
            return Err(BacktestError::invalid_argument(
                "at least one window is required",
            ));
        }
        if pairs.is_empty() {
            return Err(BacktestError::invalid_argument(
                "at least one trading pair is required",
            ));
        }
        let window_numbers = parsed_windows
            .iter()
            .map(|(number, _)| *number)
            .collect::<Vec<_>>();
        let data = Series::load(&market_data, &pairs, &window_numbers)?;
        let (driving_number, driving_window) = *parsed_windows
            .iter()
            .min_by_key(|(_, window)| *window)
            .ok_or_else(|| BacktestError::invalid_argument("no driving window is available"))?;
        let market_slots = MarketSlot::build_all(&strategy_context, &data, &pairs)?;

        let steps = market_slots
            .iter()
            .map(|slot| slot.series[&driving_number].candles.len())
            .min()
            .unwrap_or_default();
        if steps == 0 {
            return Err(BacktestError::invalid_argument(
                "market data has no simulation steps",
            ));
        }
        let driving_millis = driving_window.millis().upper_bound();
        if *open_candle_progress_millis >= driving_millis {
            return Err(BacktestError::invalid_argument(
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

    fn read_dto<T: Message + Default>(path: &str, input: Input) -> Result<T, BacktestError> {
        let bytes = fs::read(path).map_err(|source| BacktestError::ReadInput {
            input,
            path: PathBuf::from(path),
            source,
        })?;
        T::decode(&bytes[..]).map_err(|source| BacktestError::DecodeInput {
            input,
            path: PathBuf::from(path),
            source,
        })
    }
}
