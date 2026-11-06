use platform_exit_codes::ExitCode;
use platform_runner::{
    command::{Command, SimulationParameters},
    convert::string::JsonString,
    generated::dto::{MarketCandleDto, MarketOhlcvDto, PortfolioDto, StrategyContextDto},
};
use prost::Message;
use std::{collections::BTreeMap, fs};
use stock_trek::{
    errors::StockTrekError,
    markets::{AlignedWindow, TimestampMillis},
    types::{AssetId, CexId},
};

use super::Balances;

type MarketData = BTreeMap<CexId, BTreeMap<(AssetId, AssetId), BTreeMap<u32, Series>>>;

#[derive(Clone)]
pub(super) struct Series {
    pub(super) candles: Vec<MarketCandleDto>,
    pub(super) ends: Vec<u64>,
}

pub(super) struct MarketSlot {
    pub(super) cex: CexId,
    pub(super) cex_key: String,
    pub(super) pair: (AssetId, AssetId),
    pub(super) base_index: usize,
    pub(super) quote_index: usize,
    pub(super) series: BTreeMap<u32, Series>,
}

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
            return Err(invalid_argument("simulation count must be at least one"));
        }
        for (name, chance) in [
            ("candle loss chance", simulation.candle_loss_chance),
            ("slippage chance", simulation.slippage_chance),
            ("lost order chance", simulation.lost_order_chance),
        ] {
            if !(0.0..=1.0).contains(&chance) {
                return Err(invalid_argument(&format!(
                    "{name} must be between zero and one"
                )));
            }
        }
        if !(0.0..=1.0).contains(&simulation.slippage_proportion) {
            return Err(invalid_argument(
                "slippage proportion must be between zero and one",
            ));
        }
        let strategy_context: StrategyContextDto = read_dto(
            strategy_context_path,
            ExitCode::ServerRunnerStrategyContextFileNotFound,
            ExitCode::ServerRunnerStrategyContextNotDecoded,
        )?;
        let portfolio: PortfolioDto = read_dto(
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
        let market_data: StrategyContextDto = read_dto(
            market_data_path,
            ExitCode::ServerRunnerMarketDataFileNotFound,
            ExitCode::ServerRunnerMarketDataNotDecoded,
        )?;
        let mut pairs = Vec::new();
        for pair in trading_pairs {
            let base: AssetId = JsonString::deserialize(&pair.base).map_err(|error| {
                invalid_argument(&format!("trading pair base is invalid: {error}"))
            })?;
            let quote: AssetId = JsonString::deserialize(&pair.quote).map_err(|error| {
                invalid_argument(&format!("trading pair quote is invalid: {error}"))
            })?;
            pairs.push((base, quote));
        }
        pairs.sort();
        pairs.dedup();
        let mut parsed_windows = Vec::new();
        for number in windows.iter().copied() {
            let repr = u8::try_from(number)
                .map_err(|_| invalid_argument(&format!("window {number} is invalid")))?;
            let window = AlignedWindow::from_repr(repr)
                .ok_or_else(|| invalid_argument(&format!("window {number} is invalid")))?;
            parsed_windows.push((number, window));
        }
        parsed_windows.sort();
        parsed_windows.dedup();
        if parsed_windows.is_empty() {
            return Err(invalid_argument("at least one window is required"));
        }
        if pairs.is_empty() {
            return Err(invalid_argument("at least one trading pair is required"));
        }
        let window_numbers = parsed_windows
            .iter()
            .map(|(number, _)| *number)
            .collect::<Vec<_>>();
        let data = load_market_data(&market_data, &pairs, &window_numbers)?;
        let driving_window = *parsed_windows
            .iter()
            .map(|(_, window)| window)
            .min()
            .ok_or(ExitCode::ServerEntryInvalidArgument.code())?;
        let market_slots = build_slots(&strategy_context, &data, &pairs)?;
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
            return Err(invalid_argument("market data has no simulation steps"));
        }
        let driving_millis = driving_window.millis().upper_bound();
        if *open_candle_progress_millis >= driving_millis {
            return Err(invalid_argument(
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
}

fn load_market_data(
    market_data: &StrategyContextDto,
    pairs: &[(AssetId, AssetId)],
    window_numbers: &[u32],
) -> Result<MarketData, i32> {
    let mut data: MarketData = BTreeMap::new();
    for (cex_name, asset_markets) in &market_data.exchange_asset_markets_dto {
        let cex: CexId = JsonString::deserialize(cex_name)
            .map_err(|error| not_decoded(&format!("market data cex is invalid: {error}")))?;
        for market_by_base in &asset_markets.markets_by_base_dto {
            let base: AssetId = market_by_base
                .base_asset_id_dto
                .as_ref()
                .ok_or_else(|| not_decoded("market data base asset is missing"))
                .and_then(|dto| {
                    JsonString::deserialize(&dto.asset_id).map_err(|error| {
                        not_decoded(&format!("market data base asset is invalid: {error}"))
                    })
                })?;
            for market_by_quote in &market_by_base.markets_by_quote_dto {
                let quote: AssetId = market_by_quote
                    .quote_asset_id_dto
                    .as_ref()
                    .ok_or_else(|| not_decoded("market data quote asset is missing"))
                    .and_then(|dto| {
                        JsonString::deserialize(&dto.asset_id).map_err(|error| {
                            not_decoded(&format!("market data quote asset is invalid: {error}"))
                        })
                    })?;
                if !pairs.contains(&(base, quote)) {
                    continue;
                }
                let Some(market) = market_by_quote.market_dto.as_ref() else {
                    return Err(not_decoded("market data market is missing"));
                };
                for (number, candles_dto) in &market.aligned_dto {
                    if !window_numbers.contains(number) {
                        continue;
                    }
                    let mut candles = candles_dto.candles_dto.clone();
                    candles.sort_by_key(|candle| candle.start_time_millis_inc);
                    validate_candles(&candles)?;
                    let window = AlignedWindow::from_repr(
                        u8::try_from(*number)
                            .map_err(|_| not_decoded("market data window is invalid"))?,
                    )
                    .ok_or_else(|| not_decoded("market data window is invalid"))?;
                    let ends = candles
                        .iter()
                        .map(|candle| candle_end(candle, window))
                        .collect::<Result<Vec<u64>, i32>>()?;
                    data.entry(cex)
                        .or_default()
                        .entry((base, quote))
                        .or_default()
                        .insert(*number, Series { candles, ends });
                }
            }
        }
    }
    for cex in data.keys().copied().collect::<Vec<_>>() {
        for pair in pairs {
            if !data[&cex].contains_key(pair) {
                return Err(invalid_argument(&format!(
                    "market data has no {pair:?} series for {cex}"
                )));
            }
        }
        for ((base, quote), series) in &data[&cex] {
            for number in window_numbers {
                if !series.contains_key(number) {
                    return Err(invalid_argument(&format!(
                        "market data has no window {number} series for {cex} {base}/{quote}"
                    )));
                }
            }
        }
    }
    Ok(data)
}

fn candle_end(candle: &MarketCandleDto, window: AlignedWindow) -> Result<u64, i32> {
    window
        .bounds_for(TimestampMillis::new(candle.start_time_millis_inc))
        .map(|bounds| bounds.upper().as_millis())
        .map_err(|error| not_decoded(&format!("market data candle bounds are invalid: {error}")))
}

fn validate_candles(candles: &[MarketCandleDto]) -> Result<(), i32> {
    for candle in candles {
        let ohlcv: &MarketOhlcvDto = candle
            .ohlcv_dto
            .as_ref()
            .ok_or_else(|| not_decoded("market data candle ohlcv is missing"))?;
        for (name, value) in [
            ("open", &ohlcv.open_dto),
            ("high", &ohlcv.high_dto),
            ("low", &ohlcv.low_dto),
            ("close", &ohlcv.close_dto),
            ("volume", &ohlcv.volume_dto),
            ("quote volume", &ohlcv.quote_volume_dto),
        ] {
            if value.is_none() {
                return Err(not_decoded(&format!(
                    "market data candle {name} is missing"
                )));
            }
        }
    }
    Ok(())
}

fn build_slots(
    strategy_context: &StrategyContextDto,
    data: &MarketData,
    pairs: &[(AssetId, AssetId)],
) -> Result<Vec<MarketSlot>, i32> {
    let mut cex_keys: BTreeMap<CexId, String> = BTreeMap::new();
    for key in strategy_context.exchange_asset_markets_dto.keys() {
        if let Ok(cex) = JsonString::deserialize::<CexId>(key) {
            cex_keys.insert(cex, key.clone());
        }
    }
    let mut slots = Vec::new();
    for (cex, pairs_data) in data {
        let Some(cex_key) = cex_keys.get(cex) else {
            return Err(invalid_argument(&format!(
                "strategy context has no {cex} markets"
            )));
        };
        for pair in pairs {
            let (base_index, quote_index) = find_slot(strategy_context, cex_key, pair)?;
            slots.push(MarketSlot {
                cex: *cex,
                cex_key: cex_key.clone(),
                pair: *pair,
                base_index,
                quote_index,
                series: pairs_data[pair].clone(),
            });
        }
    }
    Ok(slots)
}

fn find_slot(
    strategy_context: &StrategyContextDto,
    cex_key: &str,
    pair: &(AssetId, AssetId),
) -> Result<(usize, usize), i32> {
    let Some(asset_markets) = strategy_context.exchange_asset_markets_dto.get(cex_key) else {
        return Err(invalid_argument(&format!(
            "strategy context has no {cex_key} markets"
        )));
    };
    let base_index = asset_markets
        .markets_by_base_dto
        .iter()
        .position(|market_by_base| {
            market_by_base
                .base_asset_id_dto
                .as_ref()
                .and_then(|dto| JsonString::deserialize::<AssetId>(&dto.asset_id).ok())
                == Some(pair.0)
        })
        .ok_or_else(|| {
            invalid_argument(&format!(
                "strategy context has no {cex_key} {} market",
                pair.0
            ))
        })?;
    let quote_index = asset_markets.markets_by_base_dto[base_index]
        .markets_by_quote_dto
        .iter()
        .position(|market_by_quote| {
            market_by_quote
                .quote_asset_id_dto
                .as_ref()
                .and_then(|dto| JsonString::deserialize::<AssetId>(&dto.asset_id).ok())
                == Some(pair.1)
        })
        .ok_or_else(|| {
            invalid_argument(&format!(
                "strategy context has no {cex_key} {}/{} market",
                pair.0, pair.1
            ))
        })?;
    Ok((base_index, quote_index))
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

fn invalid_argument(message: &str) -> i32 {
    eprintln!("cwasm-invoker: {message}");
    ExitCode::ServerEntryInvalidArgument.code()
}

fn not_decoded(message: &str) -> i32 {
    eprintln!("cwasm-invoker: {message}");
    ExitCode::ServerRunnerMarketDataNotDecoded.code()
}
