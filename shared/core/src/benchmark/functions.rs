use super::{round_down_nth_decimal, round_nth_decimal, BenchmarkTradeError};
use crate::benchmark::{
    count_decimal_places, new_benchmark_trade, BenchmarkTrade, NewBenchmarkTradeParams, PriceLock,
};
use crate::trader::Trader;
use common::enums::order_type::OrderType;
use common::enums::side::Side;
use common::enums::signal_category::SignalCategory;
use common::functions::{get_price_columns_f32, get_signal_col_values};
use common::traits::exchange::{BenchmarkExchange, TraderHelper};
use glow_error::GlowError;
use polars::prelude::*;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
enum IterationsError {
    ZeroUnits,
    InsufficientFunds,
}

#[derive(Clone, Debug)]
struct IterationData {
    fee: f32,
    units: f32,
    pnl: f32,
    roi: f32,
    balance: f32,
    funding: f32,
    position: i32,
    action: String,
}

impl IterationData {
    pub fn new(
        fee: f32,
        units: f32,
        pnl: f32,
        roi: f32,
        balance: f32,
        funding: f32,
        position: i32,
        action: String,
    ) -> Self {
        Self {
            fee,
            units,
            pnl,
            roi,
            balance,
            funding,
            position,
            action,
        }
    }
}

pub fn compute_benchmark_positions(
    trader: &Trader,
    initial_strategy_df: DataFrame,
) -> Result<DataFrame, GlowError> {
    // let data = data.to_owned();
    // TODO: TRY TO IMPLEMENT THIS USING LAZYFRAMES
    let perf_start = Instant::now();

    let mut df = initial_strategy_df;
    let df_height = df.height();

    let base_symbol = trader.trader_exchange.get_base_symbol();
    let base_contract = trader.trader_exchange.get_base_contract();
    let (opens, highs, lows, closes) = get_price_columns_f32(&df, &base_symbol)?;
    let shorts = get_signal_col_values(&df, SignalCategory::GoShort)?;
    let longs = get_signal_col_values(&df, SignalCategory::GoShort)?;
    let close_shorts = get_signal_col_values(&df, SignalCategory::CloseShort)?;
    let close_longs = get_signal_col_values(&df, SignalCategory::CloseLong)?;

    let mut trade_fees = vec![0.0];
    let mut units = vec![0.0];
    let mut profit_and_loss = vec![0.0];
    let mut returns = vec![0.0];
    let mut balances = vec![100.0];
    let mut fundings = vec![0_f32];
    let mut positions = vec![0];
    let mut actions = vec![SignalCategory::KeepPosition.get_column().to_owned()];
    let trading_settings = trader.trader_exchange.get_trading_settings();
    let leverage_factor = trading_settings.leverage.get_factor() as f32;
    let has_leverage = leverage_factor > 1.0;

    let price_level_modifier_map_binding = trading_settings.price_level_modifier_map.clone();
    let stop_loss: Option<PriceLock> = price_level_modifier_map_binding
        .get("sl").map(|sl| sl.clone().into());
    let take_profit: Option<PriceLock> = price_level_modifier_map_binding
        .get("tp").map(|tp| tp.clone().into());
    let should_check_price_modifiers = has_leverage || stop_loss.is_some() || take_profit.is_some();

    let maker_fee_rate = trader.trader_exchange.get_maker_fee() as f32;
    let taker_fee_rate = trader.trader_exchange.get_taker_fee() as f32;
    let open_order_fee_rate = if trading_settings.order_types.0 == OrderType::Market {
        taker_fee_rate
    } else {
        maker_fee_rate
    };
    let close_order_fee_rate = if trading_settings.order_types.1 == OrderType::Market {
        taker_fee_rate
    } else {
        maker_fee_rate
    };
    let order_sizes = (
        base_contract.minimum_order_size as f32,
        if trading_settings.order_types.0 == OrderType::Market {
            base_contract.maximum_order_sizes.0 as f32
        } else {
            base_contract.maximum_order_sizes.1 as f32
        },
    );
    let tick_size = base_contract.tick_size;
    let price_locks = (stop_loss, take_profit);
    let minimum_notional_value = trader
        .trader_exchange
        .get_minimum_notional_value()
        .map(|v| v as f32);

    let mut current_trade: Option<BenchmarkTrade> = None;
    // let mut current_peak_returns = 0.0;
    let mut current_min_price_threshold = None;
    let mut current_max_price_threshold = None;
    let symbol_decimals = count_decimal_places(order_sizes.0);
    let tick_decimals = count_decimal_places(tick_size as f32);
    let allocation_pct = trading_settings.allocation_percentage as f32;

    // need to be updated
    // trade_fees, units, profit_and_loss, returns, balances, positions, actions
    let mut index = 0;

    while index < df_height {
        if index == 0 {
            index += 1;
            continue;
        }

        let current_position = positions[index - 1];
        let current_units = units[index - 1];
        let current_balance = balances[index - 1];
        let current_funding = fundings[index - 1];

        let default_results = IterationData::new(
            0_f32,
            current_units,
            0_f32,
            0_f32,
            current_balance,
            current_funding,
            current_position,
            SignalCategory::KeepPosition.get_column().to_owned(),
        );

        let result: Result<IterationData, IterationsError> = if current_position == 0 {
            let should_short = shorts[index - 1] == 1;
            let should_long = longs[index - 1] == 1;
            if should_short || should_long {
                let open_price = opens[index];
                let close_price = closes[index];
                let new_benchmark_trade_params = NewBenchmarkTradeParams::new(
                    allocation_pct,
                    current_balance,
                    leverage_factor,
                    minimum_notional_value,
                    open_order_fee_rate,
                    order_sizes,
                    open_price,
                    price_locks,
                    if should_short { Side::Sell } else { Side::Buy },
                    symbol_decimals,
                    taker_fee_rate,
                    tick_decimals,
                );
                let trade_result_params = TradeResultParams::new(
                    close_price,
                    close_order_fee_rate,
                    current_balance,
                    current_funding,
                    should_short,
                );
                match new_benchmark_trade(new_benchmark_trade_params) {
                    Ok(trade) => {
                        let params = OnOpenTradeParams::new(trade_result_params, trade);
                        let result = on_open_trade(
                            params,
                            &mut current_trade,
                            &mut current_min_price_threshold,
                            &mut current_max_price_threshold,
                        );
                        Ok(result)
                    }
                    Err(error) => {
                        let result = on_open_trade_error(
                            error,
                            new_benchmark_trade_params,
                            trade_result_params,
                            &mut current_trade,
                            &mut current_min_price_threshold,
                            &mut current_max_price_threshold,
                        );
                        if let Err(error) = result {
                            Err(error)
                        } else {
                            Ok(result.unwrap())
                        }
                    }
                }
            } else {
                Ok(default_results)
            }
        } else {
            let trade = &current_trade.unwrap();
            let current_side = trade.side;
            let stopped_result = if should_check_price_modifiers {
                let min_price = lows[index];
                let max_price = highs[index];
                let binds_on_min_price =
                    min_price <= current_min_price_threshold.unwrap_or_default();
                let binds_on_max_price = !binds_on_min_price
                    && max_price >= current_max_price_threshold.unwrap_or_default();

                if binds_on_min_price || binds_on_max_price {
                    // let prev_close_price = closes[index - 1];
                    // let prev_end_timestamp = end_timestamps[index - 1];
                    let (pnl, roi, close_fee) = trade.get_pnl_returns_and_fees(
                        if binds_on_min_price {
                            current_min_price_threshold.unwrap()
                        } else {
                            current_max_price_threshold.unwrap()
                        },
                        close_order_fee_rate,
                    );
                    let action = match (current_side, binds_on_max_price, binds_on_min_price) {
                        (Side::Buy, true, _) => SignalCategory::TakeProfit,
                        (Side::Buy, _, true) => {
                            if trade.prices.2.is_some() {
                                SignalCategory::StopLoss
                            } else {
                                SignalCategory::TakeProfit
                            }
                        }
                        (Side::Sell, true, _) => {
                            if trade.prices.2.is_some() {
                                SignalCategory::StopLoss
                            } else {
                                SignalCategory::TakeProfit
                            }
                        }
                        (Side::Sell, _, true) => SignalCategory::TakeProfit,
                        (_, _, _) => unreachable!(),
                    };
                    let result = IterationData::new(
                        close_fee,
                        0.0,
                        pnl,
                        roi,
                        f32::max(
                            0.0,
                            round_nth_decimal(
                                current_balance + trade.initial_margin + pnl,
                                tick_decimals,
                            ),
                        ),
                        current_funding,
                        0,
                        action.get_column().to_owned(),
                    );
                    (current_min_price_threshold, current_max_price_threshold) = (None, None);
                    current_trade = None;
                    Some(result)
                } else {
                    None
                }
            } else {
                None
            };

            if let Some(stopped_result) = stopped_result {
                Ok(stopped_result)
            } else {
                let open_price = opens[index];
                let (pnl, roi, close_fee) =
                    trade.get_pnl_returns_and_fees(open_price, close_order_fee_rate);
                let was_short_closed = close_shorts[index - 1] == 1 && current_side == Side::Sell;
                let was_long_closed = close_longs[index - 1] == 1 && current_side == Side::Buy;

                let (close_fee, units, balance, position, action) =
                    if was_short_closed || was_long_closed {
                        (current_min_price_threshold, current_max_price_threshold) = (None, None);
                        current_trade = None;
                        (
                            close_fee,
                            0_f32,
                            f32::max(
                                0.0,
                                round_nth_decimal(
                                    current_balance + trade.initial_margin + pnl,
                                    tick_decimals,
                                ),
                            ),
                            0,
                            if was_short_closed {
                                SignalCategory::CloseShort.get_column().to_owned()
                            } else {
                                SignalCategory::CloseLong.get_column().to_owned()
                            },
                        )
                    } else {
                        (
                            0_f32,
                            current_units,
                            current_balance,
                            current_position,
                            default_results.action,
                        )
                    };

                Ok(IterationData::new(
                    close_fee,
                    units,
                    pnl,
                    roi,
                    balance,
                    current_funding,
                    position,
                    action,
                ))
            }
        };

        if result.is_err() {
            println!("result is error {:?}", result);
            index += 1;

            break;
        }

        let IterationData {
            fee,
            units: iteration_units,
            pnl,
            roi,
            balance,
            funding,
            position,
            action,
        } = result.unwrap();

        trade_fees.push(fee);
        units.push(iteration_units);
        profit_and_loss.push(pnl);
        returns.push(roi);
        balances.push(balance);
        fundings.push(funding);
        positions.push(position);
        actions.push(action);
        index += 1;
    }

    if index < df_height {
        let missing_data_no = df_height - index + 1;

        let last_fee = trade_fees.last().unwrap().clone();
        trade_fees.extend(vec![last_fee; missing_data_no]);
        let last_units = units.last().unwrap().clone();
        units.extend(vec![last_units; missing_data_no]);
        let last_pnl = profit_and_loss.last().unwrap().clone();
        profit_and_loss.extend(vec![last_pnl; missing_data_no]);
        let last_roi = returns.last().unwrap().clone();
        returns.extend(vec![last_roi; missing_data_no]);
        let last_balance = balances.last().unwrap().clone();
        balances.extend(vec![last_balance; missing_data_no]);
        let last_funding = fundings.last().unwrap().clone();
        fundings.extend(vec![last_funding; missing_data_no]);
        let last_position = positions.last().unwrap().clone();
        positions.extend(vec![last_position; missing_data_no]);
        let last_action = actions.last().unwrap().clone();
        actions.extend(vec![last_action; missing_data_no]);
    }

    // if last position was taken
    if positions.last().unwrap() != &0 {
        if let Some((before_last_order_index, _)) = positions // over positions vector
            .iter() // iterate over
            .enumerate() // an enumeration
            .rev() // of reversed positions
            .find(|(_, value)| value == &&0)
        // until it finds where value is 0
        {
            // splices results vectors to values before opening the order
            // note that even though the vector was reversed, before_last_order_index keeps being the original vector index. Thanks, Rust <3
            let range = before_last_order_index..df_height;
            let zeroed_float_patch: Vec<f32> = range.clone().map(|_| 0.0 as f32).collect();
            let zeroed_integer_patch: Vec<i32> = range.clone().map(|_| 0 as i32).collect();
            let keep_position_action_patch: Vec<String> = range
                .clone()
                .map(|_| SignalCategory::KeepPosition.get_column().to_owned())
                .collect();

            trade_fees.splice(range.clone(), zeroed_float_patch.clone());
            units.splice(range.clone(), zeroed_float_patch.clone());
            profit_and_loss.splice(range.clone(), zeroed_float_patch.clone());

            positions.splice(range.clone(), zeroed_integer_patch);
            actions.splice(range.clone(), keep_position_action_patch);

            let previous_balance = balances[before_last_order_index];
            let patch_balances: Vec<f32> = range.clone().map(|_| previous_balance as f32).collect();
            balances.splice(range.clone(), patch_balances);
            let previous_funding = fundings[before_last_order_index];
            let patch_fundings: Vec<f32> = range.clone().map(|_| previous_funding as f32).collect();
            fundings.splice(range.clone(), patch_fundings);
            returns.splice(range.clone(), zeroed_float_patch);
        }
    }

    let elapsed_time = perf_start.elapsed();
    let elapsed_millis = elapsed_time.as_nanos();
    println!(
        "compute_benchmark_positions => Elapsed time (ms): {}",
        elapsed_millis as f64 / 1_000_000_f64
    );

    let trade_fee_series = Series::new(
        "trade_fees".into(),
        trade_fees.iter().map(|&f| f as f64).collect::<Vec<f64>>(),
    );
    let units_series = Series::new(
        "units".into(),
        units.iter().map(|&u| u as f64).collect::<Vec<f64>>(),
    );
    let profit_and_loss_series = Series::new(
        "profit_and_loss".into(),
        profit_and_loss
            .iter()
            .map(|&p| p as f64)
            .collect::<Vec<f64>>(),
    );
    let returns_series = Series::new(
        "returns".into(),
        returns.iter().map(|&r| r as f64).collect::<Vec<f64>>(),
    );
    let balance_series = Series::new(
        "balance".into(),
        balances
            .iter()
            .zip(fundings.iter())
            .map(|(&a, &b)| (a + b) as f64)
            .collect::<Vec<f64>>(),
    );
    let position_series = Series::new("position".into(), positions);
    let action_series = Series::new("action".into(), actions);

    let df = df.with_column(trade_fee_series)?;
    let df = df.with_column(units_series)?;
    let df = df.with_column(profit_and_loss_series)?;
    let df = df.with_column(returns_series)?;
    let df = df.with_column(balance_series)?;
    let df = df.with_column(position_series)?;
    let df = df.with_column(action_series)?;

    Ok(df.clone())
}

#[derive(Clone, Copy)]
struct TradeResultParams {
    pub close_price: f32,
    pub close_order_fee_rate: f32,
    pub current_balance: f32,
    pub current_funding: f32,
    pub should_short: bool,
}

impl TradeResultParams {
    pub fn new(
        close_price: f32,
        close_order_fee_rate: f32,
        current_balance: f32,
        current_funding: f32,
        should_short: bool,
    ) -> Self {
        Self {
            close_price,
            close_order_fee_rate,
            current_balance,
            current_funding,
            should_short,
        }
    }
}
#[derive(Clone, Copy)]
struct OnOpenTradeParams {
    pub close_price: f32,
    pub close_order_fee_rate: f32,
    pub current_balance: f32,
    pub current_funding: f32,
    pub should_short: bool,
    pub trade: BenchmarkTrade,
    pub tick_decimals: i32,
}

impl OnOpenTradeParams {
    pub fn new(result_params: TradeResultParams, trade: BenchmarkTrade) -> Self {
        let TradeResultParams {
            close_price,
            close_order_fee_rate,
            current_balance,
            current_funding,
            should_short,
        } = result_params;
        Self {
            close_price,
            close_order_fee_rate,
            current_balance,
            current_funding,
            // remainder: success_params.1,
            should_short,
            trade,
            tick_decimals: trade.tick_decimals,
        }
    }
}

fn on_open_trade(
    params: OnOpenTradeParams,
    current_trade: &mut Option<BenchmarkTrade>,
    current_min_price_threshold: &mut Option<f32>,
    current_max_price_threshold: &mut Option<f32>,
) -> IterationData {
    let OnOpenTradeParams {
        close_price,
        close_order_fee_rate,
        current_balance,
        current_funding,
        // remainder,
        should_short,
        trade,
        tick_decimals,
    } = params;
    (*current_min_price_threshold, *current_max_price_threshold) = trade.get_threshold_prices();
    let side = trade.side.into();
    let open_fee = trade.open_fee;
    let units = trade.units;
    let (pnl, roi, _) = trade.get_pnl_returns_and_fees(close_price, close_order_fee_rate);
    *current_trade = Some(trade);
    IterationData::new(
        open_fee,
        units,
        pnl,
        roi,
        f32::max(
            0.0,
            round_nth_decimal(
                current_balance - trade.initial_margin - open_fee,
                tick_decimals,
            ),
        ),
        current_funding,
        side,
        (if should_short {
            SignalCategory::GoShort
        } else {
            SignalCategory::GoLong
        })
        .get_column()
        .to_owned(),
    )
}

fn on_open_trade_error(
    error: BenchmarkTradeError,
    new_trade_params: NewBenchmarkTradeParams,
    trade_result_params: TradeResultParams,
    current_trade: &mut Option<BenchmarkTrade>,
    current_min_price_threshold: &mut Option<f32>,
    current_max_price_threshold: &mut Option<f32>,
) -> Result<IterationData, IterationsError> {
    let mut current_balance = trade_result_params.current_balance;
    let mut current_funding = trade_result_params.current_funding;
    (current_balance, current_funding) = match error {
        BenchmarkTradeError::ZeroUnits => {
            if current_funding == 0.0 {
                return Err(IterationsError::ZeroUnits);
            }
            (current_funding, 0.0)
        }
        BenchmarkTradeError::UnitsMoreThanMaxSize {
            max_expenditure,
            expenditure,
        } => {
            let suspend_amount = round_down_nth_decimal(
                expenditure - max_expenditure,
                new_trade_params.tick_decimals,
            );
            let updated_balance = round_down_nth_decimal(
                current_balance - suspend_amount,
                new_trade_params.tick_decimals,
            );
            let updated_funding = round_down_nth_decimal(
                current_funding + suspend_amount,
                new_trade_params.tick_decimals,
            );
            (updated_balance, updated_funding)
        }
        BenchmarkTradeError::UnitsLessThanMinSize { min_expenditure }
        | BenchmarkTradeError::ValueLessThanNotionalMin { min_expenditure } => {
            let total_funds = round_down_nth_decimal(
                current_funding + current_balance,
                new_trade_params.tick_decimals,
            );
            if current_funding == 0.0 || total_funds < min_expenditure {
                return Err(IterationsError::InsufficientFunds);
            }
            (total_funds, 0.0)
        }
    };
    let mut new_trade_params = new_trade_params;
    new_trade_params.current_balance = current_balance;
    let mut trade_result_params = trade_result_params;
    trade_result_params.current_balance = current_balance;
    trade_result_params.current_funding = current_funding;

    match new_benchmark_trade(new_trade_params) {
        Ok(trade) => {
            let on_open_order_params = OnOpenTradeParams::new(trade_result_params, trade);
            let result = on_open_trade(
                on_open_order_params,
                current_trade,
                current_min_price_threshold,
                current_max_price_threshold,
            );
            Ok(result)
        }
        Err(error) => on_open_trade_error(
            error,
            new_trade_params,
            trade_result_params,
            current_trade,
            current_min_price_threshold,
            current_max_price_threshold,
        ),
    }
}
