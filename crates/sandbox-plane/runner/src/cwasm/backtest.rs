use futures::SinkExt;
use platform_exit_codes::ExitCode;
use platform_runner::{
    command::{Command, SimulationParameters},
    convert::{decimal::ToDecimal, string::JsonString},
    generated::dto::{
        AssetCountDto, AssetIdDto, CexAccountPortfolioDto, CexAccountsDto, DecimalDto,
        MarketCandleDto, MarketCandlesDto, MarketOhlcvDto, PortfolioDto, StrategyContextDto,
    },
};
use prost::Message;
use rand::{Rng, SeedableRng, rngs::StdRng};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
    sync::{Arc, Mutex},
};
use stock_trek::{
    actions::ResolvedAction,
    errors::StockTrekError,
    markets::{AlignedWindow, TimestampMillis},
    types::{AccountId, AssetId, CexId, OrderRequest, Quantity, Side, Tag},
};

use super::{Emit, PORTFOLIO_FILE_NAME, STRATEGY_CONTEXT_FILE_NAME, WasmRunner};

const GUEST_ROOT: &str = "/";
const PROGRAM_NAME: &str = "cwasm-invoker";

type Balances = BTreeMap<CexId, BTreeMap<AccountId, BTreeMap<AssetId, f64>>>;
type PriceMap = BTreeMap<(CexId, AssetId, AssetId), f64>;

#[derive(Clone)]
struct Series {
    candles: Vec<MarketCandleDto>,
    ends: Vec<u64>,
}

struct MarketSlot {
    cex: CexId,
    cex_key: String,
    pair: (AssetId, AssetId),
    base_index: usize,
    quote_index: usize,
    series: BTreeMap<u32, Series>,
}

struct Spec {
    strategy_name: String,
    strategy_context: StrategyContextDto,
    initial_balances: Balances,
    market_slots: Vec<MarketSlot>,
    driving_window_number: u32,
    windows: Vec<(u32, AlignedWindow)>,
    historical_candle_count: usize,
    open_candle_progress_millis: u64,
    simulation_count: u32,
    random_seed: u64,
    simulation: SimulationParameters,
    user_id: String,
    algorithm_id: String,
    back_test_id: String,
    results_stream_url: String,
    results_post_url: String,
    steps: usize,
}

pub fn run(runner: &WasmRunner, cwasm_path: &Path, command: &Command) -> i32 {
    let spec = match Spec::new(command) {
        Ok(spec) => spec,
        Err(code) => return code,
    };
    let results = std::thread::scope(|scope| {
        (0..spec.simulation_count)
            .map(|index| {
                let spec = &spec;
                scope.spawn(move || simulate(runner, cwasm_path, spec, index))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(ExitCode::BotErrorPanic.code()))
            })
            .collect::<Vec<_>>()
    });
    results
        .into_iter()
        .find_map(Result::err)
        .unwrap_or(ExitCode::Success.code())
}

fn simulate(runner: &WasmRunner, cwasm_path: &Path, spec: &Spec, index: u32) -> Result<(), i32> {
    let result = run_simulation(runner, cwasm_path, spec, index)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            eprintln!("cwasm-invoker: failed to start tokio runtime: {error}");
            ExitCode::ServerRunnerRuntimeStartFailed.code()
        })?;
    runtime.block_on(publish(spec, &result));
    Ok(())
}

async fn publish(spec: &Spec, result: &Value) {
    if let Err(error) = stream_result(&spec.results_stream_url, result).await {
        eprintln!("cwasm-invoker: failed to stream simulation result: {error}");
    }
    if let Err(error) = post_result(&spec.results_post_url, result).await {
        eprintln!("cwasm-invoker: failed to post simulation result: {error}");
    }
}

async fn stream_result(url: &str, result: &Value) -> Result<(), String> {
    let builder = tokio_websockets::ClientBuilder::new()
        .uri(url)
        .map_err(|error| format!("invalid results stream url: {error}"))?;
    let (mut stream, _) = builder
        .connect()
        .await
        .map_err(|error| format!("connection failed: {error}"))?;
    stream
        .send(tokio_websockets::Message::text(result.to_string()))
        .await
        .map_err(|error| format!("send failed: {error}"))?;
    stream
        .close()
        .await
        .map_err(|error| format!("close failed: {error}"))?;
    Ok(())
}

async fn post_result(url: &str, result: &Value) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .build()
        .map_err(|error| format!("client could not be built: {error}"))?;
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(result.to_string())
        .send()
        .await
        .map_err(|error| format!("post failed: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("post returned status {status}"));
    }
    Ok(())
}

fn run_simulation(
    runner: &WasmRunner,
    cwasm_path: &Path,
    spec: &Spec,
    index: u32,
) -> Result<Value, i32> {
    let mut rng = StdRng::seed_from_u64(spec.random_seed.wrapping_add(u64::from(index)));
    let mut balances = spec.initial_balances.clone();
    let directory = tempfile::Builder::new()
        .prefix("platform-backtest-")
        .tempdir()
        .map_err(|error| {
            eprintln!("cwasm-invoker: failed to create scratch pad: {error}");
            ExitCode::BotErrorCreatingScratchPad.code()
        })?;
    let mut steps = Vec::new();
    for step in 0..spec.steps {
        let (context, prices, timestamp) = step_context(spec, step, &mut rng)?;
        let portfolio = portfolio_dto(&balances);
        write_guest_input(directory.path(), STRATEGY_CONTEXT_FILE_NAME, &context)?;
        write_guest_input(directory.path(), PORTFOLIO_FILE_NAME, &portfolio)?;
        let events = execute_step(
            runner,
            cwasm_path,
            spec,
            directory.path(),
            &mut balances,
            &prices,
            &mut rng,
        )?;
        steps.push(json!({
            "timestamp": timestamp,
            "assetTotals": asset_totals(&balances),
            "events": events,
        }));
    }
    Ok(json!({
        "userId": spec.user_id,
        "algorithmId": spec.algorithm_id,
        "backTestId": spec.back_test_id,
        "simulationId": format!("{}-{index}", spec.back_test_id),
        "simulationSeed": spec.random_seed.wrapping_add(u64::from(index)),
        "initialAssetTotals": asset_totals(&spec.initial_balances),
        "steps": steps,
    }))
}

fn write_guest_input<T: Message>(directory: &Path, name: &str, message: &T) -> Result<(), i32> {
    let path = directory.join(name);
    fs::write(&path, message.encode_to_vec()).map_err(|error| {
        eprintln!("cwasm-invoker: failed to write guest input {path:?}: {error}");
        ExitCode::BotErrorCreatingScratchPad.code()
    })
}

fn execute_step(
    runner: &WasmRunner,
    cwasm_path: &Path,
    spec: &Spec,
    directory: &Path,
    balances: &mut Balances,
    prices: &PriceMap,
    rng: &mut StdRng,
) -> Result<Vec<Value>, i32> {
    let (emit, collected) = collector();
    let arguments = [
        PROGRAM_NAME,
        "Execute",
        spec.strategy_name.as_str(),
        &guest_path(STRATEGY_CONTEXT_FILE_NAME),
        &guest_path(PORTFOLIO_FILE_NAME),
    ];
    let code = runner.execute_arguments(cwasm_path, &arguments, directory, &emit);
    if code != ExitCode::Success.code() {
        eprintln!("cwasm-invoker: guest step failed with exit code {code}");
        return Err(code);
    }
    let lines = std::mem::take(
        &mut *collected
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    );
    let mut events = Vec::new();
    for (kind, payload) in lines {
        if kind != "action" {
            continue;
        }
        let Some(action_json) = payload.get("action").and_then(Value::as_str) else {
            eprintln!("cwasm-invoker: action event is missing its action field");
            return Err(ExitCode::BotErrorResolvingAction.code());
        };
        let action: ResolvedAction = serde_json::from_str(action_json).map_err(|error| {
            eprintln!("cwasm-invoker: action could not be decoded: {error}");
            ExitCode::BotErrorResolvingAction.code()
        })?;
        events.push(handle_action(
            &action,
            balances,
            prices,
            rng,
            &spec.simulation,
        )?);
    }
    Ok(events)
}

fn handle_action(
    action: &ResolvedAction,
    balances: &mut Balances,
    prices: &PriceMap,
    rng: &mut StdRng,
    parameters: &SimulationParameters,
) -> Result<Value, i32> {
    match action {
        ResolvedAction::PlaceOrder {
            cex_id,
            account_id,
            order_request,
        } => place_order(
            cex_id,
            account_id,
            order_request,
            balances,
            prices,
            rng,
            parameters,
        ),
        ResolvedAction::CancelAllOrders { tag } => Ok(cancel_event(None, None, tag)),
        ResolvedAction::CancelAllOrdersInCex { cex_id, tag } => {
            Ok(cancel_event(Some(cex_id), None, tag))
        }
        ResolvedAction::CancelAllOrdersInCexAccount {
            cex_id,
            account_id,
            tag,
        } => Ok(cancel_event(Some(cex_id), Some(account_id), tag)),
        _ => {
            eprintln!("cwasm-invoker: unsupported action in back test");
            Err(ExitCode::BotErrorResolvingAction.code())
        }
    }
}

fn place_order(
    cex_id: &CexId,
    account_id: &AccountId,
    order_request: &OrderRequest<AssetId, f64>,
    balances: &mut Balances,
    prices: &PriceMap,
    rng: &mut StdRng,
    parameters: &SimulationParameters,
) -> Result<Value, i32> {
    if rng.gen_range(0.0..1.0) < parameters.lost_order_chance {
        return Ok(json!({
            "type": "order_lost",
            "cex": cex_id.to_string(),
            "account": account_id.0,
            "orderRequest": order_value(order_request),
        }));
    }
    let assets = account_balances(balances, *cex_id, account_id);
    match order_request {
        OrderRequest::MarketBuy {
            base,
            quote,
            quote_quantity,
            ..
        } => {
            let (price, slipped) = slipped_price(
                prices,
                *cex_id,
                base,
                quote,
                Some(&Side::Buy),
                rng,
                parameters,
            )?;
            let available = assets.get(quote).copied().unwrap_or(0.0);
            let spent = available.min(*quote_quantity);
            let received = spent / price;
            add_balance(assets, *quote, -spent);
            add_balance(assets, *base, received);
            Ok(order_event(
                "place_order",
                cex_id,
                account_id,
                order_request,
                price,
                received,
                spent,
                spent < *quote_quantity,
                slipped,
            ))
        }
        OrderRequest::MarketSell {
            base,
            quote,
            base_quantity,
            ..
        } => {
            let (price, slipped) = slipped_price(
                prices,
                *cex_id,
                base,
                quote,
                Some(&Side::Sell),
                rng,
                parameters,
            )?;
            let available = assets.get(base).copied().unwrap_or(0.0);
            let sold = available.min(*base_quantity);
            let received = sold * price;
            add_balance(assets, *base, -sold);
            add_balance(assets, *quote, received);
            Ok(order_event(
                "place_order",
                cex_id,
                account_id,
                order_request,
                price,
                sold,
                received,
                sold < *base_quantity,
                slipped,
            ))
        }
        OrderRequest::Limit {
            base,
            quote,
            side,
            limit_price,
            quantity,
            ..
        } => {
            let (price, slipped) =
                slipped_price(prices, *cex_id, base, quote, Some(side), rng, parameters)?;
            let crossed = match side {
                Side::Buy => price <= *limit_price,
                Side::Sell => price >= *limit_price,
            };
            if !crossed {
                return Ok(json!({
                    "type": "order_not_filled",
                    "cex": cex_id.to_string(),
                    "account": account_id.0,
                    "orderRequest": order_value(order_request),
                }));
            }
            match quantity {
                Quantity::OfBase(requested) => match side {
                    Side::Buy => {
                        let available = assets.get(quote).copied().unwrap_or(0.0);
                        let received = (available / price).min(*requested);
                        let spent = received * price;
                        add_balance(assets, *quote, -spent);
                        add_balance(assets, *base, received);
                        Ok(order_event(
                            "place_order",
                            cex_id,
                            account_id,
                            order_request,
                            price,
                            received,
                            spent,
                            received < *requested,
                            slipped,
                        ))
                    }
                    Side::Sell => {
                        let available = assets.get(base).copied().unwrap_or(0.0);
                        let sold = available.min(*requested);
                        let received = sold * price;
                        add_balance(assets, *base, -sold);
                        add_balance(assets, *quote, received);
                        Ok(order_event(
                            "place_order",
                            cex_id,
                            account_id,
                            order_request,
                            price,
                            sold,
                            received,
                            sold < *requested,
                            slipped,
                        ))
                    }
                },
                Quantity::OfQuote(requested) => match side {
                    Side::Buy => {
                        let available = assets.get(quote).copied().unwrap_or(0.0);
                        let spent = available.min(*requested);
                        let received = spent / price;
                        add_balance(assets, *quote, -spent);
                        add_balance(assets, *base, received);
                        Ok(order_event(
                            "place_order",
                            cex_id,
                            account_id,
                            order_request,
                            price,
                            received,
                            spent,
                            spent < *requested,
                            slipped,
                        ))
                    }
                    Side::Sell => {
                        let available = assets.get(base).copied().unwrap_or(0.0);
                        let received = (available * price).min(*requested);
                        let sold = received / price;
                        add_balance(assets, *base, -sold);
                        add_balance(assets, *quote, received);
                        Ok(order_event(
                            "place_order",
                            cex_id,
                            account_id,
                            order_request,
                            price,
                            sold,
                            received,
                            received < *requested,
                            slipped,
                        ))
                    }
                },
            }
        }
    }
}

fn account_balances<'a>(
    balances: &'a mut Balances,
    cex_id: CexId,
    account_id: &AccountId,
) -> &'a mut BTreeMap<AssetId, f64> {
    balances
        .entry(cex_id)
        .or_default()
        .entry(account_id.clone())
        .or_default()
}

fn add_balance(assets: &mut BTreeMap<AssetId, f64>, asset: AssetId, delta: f64) {
    let balance = assets.entry(asset).or_insert(0.0);
    *balance += delta;
    if *balance == 0.0 {
        assets.remove(&asset);
    }
}

fn slipped_price(
    prices: &PriceMap,
    cex_id: CexId,
    base: &AssetId,
    quote: &AssetId,
    side: Option<&Side>,
    rng: &mut StdRng,
    parameters: &SimulationParameters,
) -> Result<(f64, bool), i32> {
    let market_price = *prices.get(&(cex_id, *base, *quote)).ok_or_else(|| {
        eprintln!("cwasm-invoker: no market data for an order on {cex_id} {base}/{quote}");
        ExitCode::BotErrorResolvingAction.code()
    })?;
    if rng.gen_range(0.0..1.0) < parameters.slippage_chance {
        let slipped = match side {
            Some(&Side::Buy) => market_price * (1.0 + parameters.slippage_proportion),
            Some(&Side::Sell) => market_price * (1.0 - parameters.slippage_proportion),
            None => market_price,
        };
        return Ok((slipped, true));
    }
    Ok((market_price, false))
}

fn order_value(order_request: &OrderRequest<AssetId, f64>) -> Value {
    serde_json::to_value(order_request).unwrap_or(Value::Null)
}

#[allow(clippy::too_many_arguments)]
fn order_event(
    kind: &str,
    cex_id: &CexId,
    account_id: &AccountId,
    order_request: &OrderRequest<AssetId, f64>,
    fill_price: f64,
    filled_base: f64,
    filled_quote: f64,
    partial: bool,
    slipped: bool,
) -> Value {
    json!({
        "type": kind,
        "cex": cex_id.to_string(),
        "account": account_id.0,
        "orderRequest": order_value(order_request),
        "fillPrice": fill_price,
        "filledBaseQuantity": filled_base,
        "filledQuoteQuantity": filled_quote,
        "partialFill": partial,
        "slippage": slipped,
    })
}

fn cancel_event(
    cex_id: Option<&CexId>,
    account_id: Option<&AccountId>,
    tag: &Option<Tag>,
) -> Value {
    json!({
        "type": "cancel_orders",
        "cex": cex_id.map(|cex| cex.to_string()),
        "account": account_id.map(|account| account.0.clone()),
        "tag": tag.as_ref().map(|tag| tag.0.clone()),
    })
}

fn step_context(
    spec: &Spec,
    step: usize,
    rng: &mut StdRng,
) -> Result<(StrategyContextDto, PriceMap, u64), i32> {
    let mut context = spec.strategy_context.clone();
    let mut prices: PriceMap = BTreeMap::new();
    let mut timestamp = u64::MAX;
    for slot in &spec.market_slots {
        let driving = &slot.series[&spec.driving_window_number];
        let candle = &driving.candles[step];
        let now = candle.start_time_millis_inc + spec.open_candle_progress_millis;
        timestamp = timestamp.min(now);
        let ohlcv = candle
            .ohlcv_dto
            .as_ref()
            .ok_or(ExitCode::ServerRunnerMarketDataNotDecoded.code())?;
        let open_price = f64::try_from(
            ohlcv
                .open_dto
                .ok_or(ExitCode::ServerRunnerMarketDataNotDecoded.code())?,
        )
        .map_err(|_| ExitCode::ServerRunnerMarketDataNotDecoded.code())?;
        prices.insert((slot.cex, slot.pair.0, slot.pair.1), open_price);
        let Some(asset_markets) = context.exchange_asset_markets_dto.get_mut(&slot.cex_key) else {
            return Err(ExitCode::ServerEntryInvalidArgument.code());
        };
        let Some(market_by_base) = asset_markets.markets_by_base_dto.get_mut(slot.base_index)
        else {
            return Err(ExitCode::ServerEntryInvalidArgument.code());
        };
        let Some(market_by_quote) = market_by_base
            .markets_by_quote_dto
            .get_mut(slot.quote_index)
        else {
            return Err(ExitCode::ServerEntryInvalidArgument.code());
        };
        let Some(market) = market_by_quote.market_dto.as_mut() else {
            return Err(ExitCode::ServerEntryInvalidArgument.code());
        };
        let mut aligned = std::mem::take(&mut market.aligned_dto);
        for (number, window) in &spec.windows {
            let series = slot
                .series
                .get(number)
                .ok_or(ExitCode::ServerEntryInvalidArgument.code())?;
            let mut candles = window_candles(series, *window, now, spec, rng)?;
            for candle in &mut candles.candles_dto {
                candle.window = *number;
            }
            aligned.insert(*number, candles);
        }
        market.aligned_dto = aligned;
    }
    Ok((context, prices, timestamp))
}

fn window_candles(
    series: &Series,
    window: AlignedWindow,
    now: u64,
    spec: &Spec,
    rng: &mut StdRng,
) -> Result<MarketCandlesDto, i32> {
    let closed_count = series.ends.partition_point(|&end| end <= now);
    let start = closed_count.saturating_sub(spec.historical_candle_count);
    let mut candles = series.candles[start..closed_count].to_vec();
    if let Some(open_start) = open_candle_start(series, window, now)?
        && let Some(candle) = open_candle(series, open_start, now, window)?
    {
        candles.push(candle);
    }
    if spec.simulation.candle_loss_chance > 0.0 {
        candles.retain(|_| rng.gen_range(0.0..1.0) >= spec.simulation.candle_loss_chance);
    }
    Ok(MarketCandlesDto {
        candles_dto: candles,
    })
}

fn open_candle_start(series: &Series, window: AlignedWindow, now: u64) -> Result<Option<u64>, i32> {
    let bounds = window
        .bounds_for(TimestampMillis::new(now))
        .map_err(|error| {
            eprintln!("cwasm-invoker: could not compute window bounds: {error}");
            ExitCode::ServerRunnerMarketDataNotDecoded.code()
        })?;
    let lower = bounds.lower().as_millis();
    Ok(series
        .candles
        .binary_search_by_key(&lower, |candle| candle.start_time_millis_inc)
        .ok()
        .map(|_| lower))
}

fn open_candle(
    series: &Series,
    start: u64,
    now: u64,
    window: AlignedWindow,
) -> Result<Option<MarketCandleDto>, i32> {
    let index = series
        .candles
        .binary_search_by_key(&start, |candle| candle.start_time_millis_inc)
        .map_err(|_| ExitCode::ServerRunnerMarketDataNotDecoded.code())?;
    let mut candle = series.candles[index];
    candle.is_candle_closed = false;
    let window_millis = window.millis().upper_bound();
    let proportion = (now - start) as f64 / window_millis as f64;
    if let Some(ohlcv) = candle.ohlcv_dto.as_mut() {
        if let Some(volume) = ohlcv
            .volume_dto
            .take()
            .and_then(|value| scaled_decimal(value, proportion))
        {
            ohlcv.volume_dto = Some(volume);
        }
        if let Some(quote_volume) = ohlcv
            .quote_volume_dto
            .take()
            .and_then(|value| scaled_decimal(value, proportion))
        {
            ohlcv.quote_volume_dto = Some(quote_volume);
        }
    }
    candle.trade_count = (candle.trade_count as f64 * proportion) as u64;
    Ok(Some(candle))
}

fn scaled_decimal(value: DecimalDto, proportion: f64) -> Option<DecimalDto> {
    let decimal = value.to_decimal().ok()?;
    let factor = Decimal::from_f64_retain(proportion)?;
    let scaled = decimal.checked_mul(factor)?;
    Some(decimal_dto(scaled))
}

fn decimal_dto_f64(value: f64) -> DecimalDto {
    let decimal = Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO);
    decimal_dto(decimal)
}

fn decimal_dto(decimal: Decimal) -> DecimalDto {
    let unpacked = decimal.unpack();
    DecimalDto {
        lo: unpacked.lo,
        mid: unpacked.mid,
        hi: unpacked.hi,
        scale: unpacked.scale,
        negative: unpacked.negative,
    }
}

fn asset_totals(balances: &Balances) -> Value {
    serde_json::to_value(balances).unwrap_or(Value::Null)
}

fn key_string<T: serde::Serialize + std::fmt::Display>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(key)) => key,
        _ => value.to_string(),
    }
}

fn portfolio_dto(balances: &Balances) -> PortfolioDto {
    PortfolioDto {
        cex_accounts_dto: balances
            .iter()
            .map(|(cex, accounts)| {
                (
                    key_string(cex),
                    CexAccountsDto {
                        account_portfolios_dto: accounts
                            .iter()
                            .map(|(account, assets)| {
                                (
                                    account.0.clone(),
                                    CexAccountPortfolioDto {
                                        asset_counts_dto: assets
                                            .iter()
                                            .map(|(asset, total)| AssetCountDto {
                                                asset_id_dto: Some(AssetIdDto {
                                                    asset_id: key_string(asset),
                                                }),
                                                count_dto: Some(decimal_dto_f64(*total)),
                                            })
                                            .collect(),
                                        pending_orders_dto: HashMap::new(),
                                    },
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}

type Emitted = Arc<Mutex<Vec<(String, Value)>>>;

fn collector() -> (Emit, Emitted) {
    let lines: Emitted = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&lines);
    let emit: Emit = Arc::new(move |kind, payload| {
        sink.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((kind.to_string(), payload));
    });
    (emit, lines)
}

fn guest_path(name: &str) -> String {
    format!("{GUEST_ROOT}{name}")
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

impl Spec {
    fn new(command: &Command) -> Result<Self, i32> {
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

type MarketData = BTreeMap<CexId, BTreeMap<(AssetId, AssetId), BTreeMap<u32, Series>>>;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn balances() -> Balances {
        BTreeMap::from([(
            CexId::Binance,
            BTreeMap::from([(
                AccountId::new("main"),
                BTreeMap::from([(AssetId::TetherUSD, 1000.0), (AssetId::Bitcoin, 2.0)]),
            )]),
        )])
    }

    fn prices() -> PriceMap {
        BTreeMap::from([((CexId::Binance, AssetId::Bitcoin, AssetId::TetherUSD), 50.0)])
    }

    fn parameters() -> SimulationParameters {
        SimulationParameters::default()
    }

    fn market_buy(quote_quantity: f64) -> OrderRequest<AssetId, f64> {
        OrderRequest::MarketBuy {
            base: AssetId::Bitcoin,
            quote: AssetId::TetherUSD,
            activation: stock_trek::types::Activation::Immediate,
            quote_quantity,
            tag: stock_trek::types::Tag::new("test"),
        }
    }

    #[test]
    fn market_buy_fills_from_quote_balance() {
        let mut balances = balances();
        let action = ResolvedAction::PlaceOrder {
            cex_id: CexId::Binance,
            account_id: AccountId::new("main"),
            order_request: market_buy(100.0),
        };
        let event = handle_action(
            &action,
            &mut balances,
            &prices(),
            &mut StdRng::seed_from_u64(1),
            &parameters(),
        )
        .unwrap();
        assert_eq!(
            event.get("type").and_then(Value::as_str),
            Some("place_order")
        );
        assert_eq!(
            event.get("partialFill").and_then(Value::as_bool),
            Some(false)
        );
        let assets = &balances[&CexId::Binance][&AccountId::new("main")];
        assert_eq!(assets[&AssetId::TetherUSD], 900.0);
        assert_eq!(assets[&AssetId::Bitcoin], 2.0 + 2.0);
    }

    #[test]
    fn market_buy_partial_fills_when_quote_is_short() {
        let mut balances = balances();
        let accounts = balances.get_mut(&CexId::Binance).unwrap();
        let assets = accounts.get_mut(&AccountId::new("main")).unwrap();
        assets.insert(AssetId::TetherUSD, 10.0);
        let action = ResolvedAction::PlaceOrder {
            cex_id: CexId::Binance,
            account_id: AccountId::new("main"),
            order_request: market_buy(100.0),
        };
        let event = handle_action(
            &action,
            &mut balances,
            &prices(),
            &mut StdRng::seed_from_u64(1),
            &parameters(),
        )
        .unwrap();
        assert_eq!(
            event.get("partialFill").and_then(Value::as_bool),
            Some(true)
        );
        let assets = &balances[&CexId::Binance][&AccountId::new("main")];
        assert_eq!(assets.get(&AssetId::TetherUSD).copied().unwrap_or(0.0), 0.0);
        assert_eq!(assets[&AssetId::Bitcoin], 2.2);
    }

    #[test]
    fn limit_buy_fills_only_when_crossed() {
        let mut balances = balances();
        let action = ResolvedAction::PlaceOrder {
            cex_id: CexId::Binance,
            account_id: AccountId::new("main"),
            order_request: OrderRequest::Limit {
                base: AssetId::Bitcoin,
                quote: AssetId::TetherUSD,
                side: Side::Buy,
                activation: stock_trek::types::Activation::Immediate,
                limit_price: 10.0,
                time_in_force: stock_trek::types::TimeInForce::GoodTillCancelled,
                quantity: Quantity::OfBase(1.0),
                tag: stock_trek::types::Tag::new("test"),
            },
        };
        let event = handle_action(
            &action,
            &mut balances,
            &prices(),
            &mut StdRng::seed_from_u64(1),
            &parameters(),
        )
        .unwrap();
        assert_eq!(
            event.get("type").and_then(Value::as_str),
            Some("order_not_filled")
        );
        let assets = &balances[&CexId::Binance][&AccountId::new("main")];
        assert_eq!(assets[&AssetId::TetherUSD], 1000.0);
    }

    #[test]
    fn lost_order_changes_nothing() {
        let mut balances = balances();
        let action = ResolvedAction::PlaceOrder {
            cex_id: CexId::Binance,
            account_id: AccountId::new("main"),
            order_request: market_buy(100.0),
        };
        let mut parameters = parameters();
        parameters.lost_order_chance = 1.0;
        let event = handle_action(
            &action,
            &mut balances,
            &prices(),
            &mut StdRng::seed_from_u64(1),
            &parameters,
        )
        .unwrap();
        assert_eq!(
            event.get("type").and_then(Value::as_str),
            Some("order_lost")
        );
        let assets = &balances[&CexId::Binance][&AccountId::new("main")];
        assert_eq!(assets[&AssetId::TetherUSD], 1000.0);
    }

    #[test]
    fn cancel_orders_event_is_recorded() {
        let mut balances = balances();
        let action = ResolvedAction::CancelAllOrders {
            tag: Some(stock_trek::types::Tag::new("test")),
        };
        let event = handle_action(
            &action,
            &mut balances,
            &prices(),
            &mut StdRng::seed_from_u64(1),
            &parameters(),
        )
        .unwrap();
        assert_eq!(
            event.get("type").and_then(Value::as_str),
            Some("cancel_orders")
        );
    }

    #[test]
    fn asset_totals_serialize_cex_accounts_and_assets() {
        let totals = asset_totals(&balances());
        assert_eq!(
            totals["binance"]["main"]["tether_u_s_d"],
            serde_json::json!(1000.0)
        );
    }
}
