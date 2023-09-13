use crate::trader::constants::SECONDS_IN_DAY;
use crate::trader::enums::modifiers::position_lock::PositionLock;
use crate::trader::enums::order_status::OrderStatus;
use crate::trader::indicators::IndicatorWrapper;
use crate::trader::models::contract::Contract;
use crate::trader::signals::SignalWrapper;
use crate::trader::{
    enums::{
        balance::Balance, modifiers::price_level::PriceLevel, signal_category::SignalCategory,
    },
    errors::Error,
    functions::{calculate_remainder, get_symbol_ohlc_cols},
    models::{behavior_subject::BehaviorSubject, trade::Trade, trading_settings::TradingSettings},
    traits::{exchange::Exchange, indicator::Indicator, signal::Signal},
};
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::performance::Performance;

pub struct Strategy {
    pub name: String,
    pub pre_indicators: Vec<IndicatorWrapper>,
    pub indicators: Vec<IndicatorWrapper>,
    pub signals: Vec<SignalWrapper>,
    pub benchmark_balance: f64,
    performance_arc: Arc<Mutex<Performance>>,
    pub trading_settings_arc: Arc<Mutex<TradingSettings>>,
    pub exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
    current_trade_listener: BehaviorSubject<Option<Trade>>,
    pub signal_generator: BehaviorSubject<Option<SignalCategory>>,
    pub current_balance_listener: BehaviorSubject<Balance>,
}

impl Clone for Strategy {
    fn clone(&self) -> Self {
        Self::new(
            self.name.clone(),
            self.pre_indicators.clone(),
            self.indicators.clone(),
            self.signals.clone(),
            self.benchmark_balance,
            self.performance_arc.clone(),
            self.trading_settings_arc.clone(),
            self.exchange_listener.clone(),
            self.current_trade_listener.clone(),
            self.signal_generator.clone(),
            self.current_balance_listener.clone(),
        )
    }
}

impl Strategy {
    pub fn new(
        name: String,
        pre_indicators: Vec<IndicatorWrapper>,
        indicators: Vec<IndicatorWrapper>,
        signals: Vec<SignalWrapper>,
        benchmark_balance: f64,
        performance_arc: Arc<Mutex<Performance>>,
        trading_settings_arc: Arc<Mutex<TradingSettings>>,
        exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>>,
        current_trade_listener: BehaviorSubject<Option<Trade>>,
        signal_generator: BehaviorSubject<Option<SignalCategory>>,
        current_balance_listener: BehaviorSubject<Balance>,
    ) -> Strategy {
        Strategy {
            name,
            pre_indicators,
            indicators,
            signals,
            signal_generator: signal_generator.clone(),
            benchmark_balance,
            exchange_listener: exchange_listener.clone(),
            performance_arc: performance_arc.clone(),
            trading_settings_arc: trading_settings_arc.clone(),
            current_trade_listener: current_trade_listener.clone(),
            current_balance_listener: current_balance_listener.clone(),
        }
    }

    pub fn set_benchmark(
        &self,
        initial_tick_data_lf: LazyFrame,
        initial_last_bar: NaiveDateTime,
    ) -> Result<LazyFrame, Error> {
        let tick_data = initial_tick_data_lf.cache();
        let mut initial_trading_data_lf = self.set_strategy_data(tick_data)?;

        let benchmark_trading_data = self.compute_benchmark_positions(&initial_trading_data_lf)?;
        initial_trading_data_lf =
            initial_trading_data_lf.left_join(benchmark_trading_data, "start_time", "start_time");

        // get only signals from previous day
        let minus_one_day_timestamp = initial_last_bar.timestamp() * 1000 - SECONDS_IN_DAY * 1000;

        initial_trading_data_lf = initial_trading_data_lf.filter(
            col("start_time")
                .dt()
                .timestamp(TimeUnit::Milliseconds)
                .gt_eq(minus_one_day_timestamp),
        );

        Ok(initial_trading_data_lf)
    }

    pub fn set_strategy_data(&self, tick_data: LazyFrame) -> Result<LazyFrame, Error> {
        let mut lf = self.set_pre_indicators_data(&tick_data)?;
        lf = self.set_indicators_data(&lf)?;
        lf = self.set_signals_data(&lf)?;
        Ok(lf)
    }

    fn set_pre_indicators_data(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();

        for pre_indicator in &self.pre_indicators {
            let lf = pre_indicator.set_indicator_columns(data.clone())?;
            data = data.left_join(lf, "start_time", "start_time");
        }

        Ok(data)
    }

    fn set_indicators_data(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();

        for indicator in &self.indicators {
            let lf = indicator.set_indicator_columns(data.clone())?;
            data = data.left_join(lf, "start_time", "start_time");
        }

        Ok(data)
    }

    fn set_signals_data(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();
        for signal in &self.signals {
            let lf = signal.set_signal_column(&data)?;
            data = data.left_join(lf, "start_time", "start_time");
        }

        Ok(data)
    }

    fn compute_benchmark_positions(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let data = data.to_owned();
        // TODO: TRY TO IMPLEMENT THIS USING LAZYFRAMES
        let mut df = data.clone().drop_nulls(None).collect()?;

        // uses hashset to ensure no SignalCategory is double counted
        let mut signals_cols = HashSet::new();
        for signal in self.signals.clone().into_iter() {
            signals_cols.insert(String::from(signal.signal_category().get_column()));
        }
        let contains_short =
            signals_cols.contains(&SignalCategory::GoShort.get_column().to_string());
        let contains_long = signals_cols.contains(&SignalCategory::GoLong.get_column().to_string());
        let contains_short_close =
            signals_cols.contains(&SignalCategory::CloseShort.get_column().to_string());
        let contains_long_close =
            signals_cols.contains(&SignalCategory::CloseLong.get_column().to_string());
        let contains_position_close =
            signals_cols.contains(&SignalCategory::ClosePosition.get_column().to_string());

        let signals_filtered_df = df.select(&signals_cols)?;

        let mut signals_cols_map = HashMap::new();
        signals_cols.into_iter().for_each(|col| {
            signals_cols_map.insert(
                col.clone(),
                signals_filtered_df
                    .column(&col)
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<i32>>(),
            );
        });

        let exchange_ref = self.exchange_listener.ref_value();
        let exchange_fee_rate = exchange_ref.get_maker_fee();
        let traded_contract = exchange_ref.get_traded_contract();

        let (_, high_col, low_col, price_col) = get_symbol_ohlc_cols(&traded_contract.symbol);
        let additional_cols = vec![price_col.clone(), high_col.clone(), low_col.clone()];

        let additional_filtered_df = df.select(&additional_cols)?;
        let mut additional_cols_map = HashMap::new();
        additional_cols.into_iter().for_each(|col| {
            additional_cols_map.insert(
                col.clone(),
                additional_filtered_df
                    .column(&col)
                    .unwrap()
                    .f64()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<f64>>(),
            );
        });

        let start_time = Instant::now();
        let mut current_order: Option<LeveragedOrder> = None;

        // fee = balance * leverage * price * fee_rate
        // marginal fee = price * fee_rate
        let mut trade_fees = vec![0.0];
        let mut units = vec![0.0];
        let mut profit_and_loss = vec![0.0];
        let mut returns = vec![0.0];
        let mut balances = vec![self.benchmark_balance];
        let mut positions = vec![0];
        let mut actions = vec![SignalCategory::KeepPosition.get_column().to_string()];

        let settings_guard = self
            .trading_settings_arc
            .lock()
            .expect("compute_benchmark_positions -> trading settings settings_guard unwrap");

        let leverage_factor = settings_guard.leverage.get_factor();
        let has_leverage = if leverage_factor > 1.0 { true } else { false };

        let price_level_modifier_map_binding = settings_guard.price_level_modifier_map.clone();

        let stop_loss_opt: Option<&PriceLevel> =
            price_level_modifier_map_binding.get(&PriceLevel::StopLoss(0.0).get_hash_key());
        let take_profit_opt =
            price_level_modifier_map_binding.get(&PriceLevel::TakeProfit(0.0).get_hash_key());
        let dataframe_height = df.height();

        let position_modifier = settings_guard.position_lock_modifier.clone();

        drop(settings_guard);

        for index in 0..dataframe_height {
            if index == 0 {
                continue;
            }

            let current_position = positions[index - 1];
            let current_units = units[index - 1];
            let current_balance = balances[index - 1];

            // position is neutral
            if current_position == 0 {
                // and changed to short
                if contains_short && signals_cols_map.get("short").unwrap()[index] == 1 {
                    let current_price = additional_cols_map.get(&price_col).unwrap()[index];
                    let order = take_position_by_balance(
                        traded_contract,
                        -1,
                        current_price,
                        current_balance,
                        exchange_fee_rate,
                        leverage_factor,
                    );

                    if order.position == -1 {
                        trade_fees.push(order.open_fee);
                        units.push(order.units);
                        profit_and_loss.push(0.0);
                        let (_, _, _, _, current_returns) =
                            order.get_close_data(current_price).unwrap();
                        returns.push(current_returns);
                        balances.push(f64::max(
                            0.0,
                            current_balance - order.get_order_cost(None) + order.balance_remainder,
                        ));
                        positions.push(order.position);
                        actions.push(SignalCategory::GoShort.get_column().to_string());
                        current_order = Some(order);
                        continue;
                    }
                }
                // and changed to long
                if contains_long && signals_cols_map.get("long").unwrap()[index] == 1 {
                    let current_price = additional_cols_map.get(&price_col).unwrap()[index];
                    let order = take_position_by_balance(
                        traded_contract,
                        1,
                        current_price,
                        current_balance,
                        exchange_fee_rate,
                        leverage_factor,
                    );

                    if order.position == 1 {
                        trade_fees.push(order.open_fee);
                        units.push(order.units);
                        profit_and_loss.push(0.0);
                        let (_, _, _, _, current_returns) =
                            order.get_close_data(current_price).unwrap();
                        returns.push(current_returns);
                        balances.push(f64::max(
                            0.0,
                            current_balance - order.get_order_cost(None) + order.balance_remainder,
                        ));
                        positions.push(order.position);
                        actions.push(SignalCategory::GoLong.get_column().to_string());
                        current_order = Some(order);
                        continue;
                    }
                }
                returns.push(0.0);
                // and kept neutral
            } else {
                let mut order = current_order.clone().unwrap().clone();
                let current_price = additional_cols_map.get(&price_col).unwrap()[index];
                let (_, _, _, _, current_returns) = order.get_close_data(current_price).unwrap();
                returns.push(current_returns);
                if has_leverage || stop_loss_opt.is_some() || take_profit_opt.is_some() {
                    let min_price = additional_cols_map.get(&low_col).unwrap()[index];
                    let max_price = additional_cols_map.get(&high_col).unwrap()[index];
                    match order.check_price_level_modifiers(
                        min_price,
                        max_price,
                        current_price,
                        stop_loss_opt,
                        take_profit_opt,
                    ) {
                        Ok(order_result) => {
                            trade_fees.push(order_result.close_fee);
                            units.push(0.0);
                            profit_and_loss.push(order_result.profit_and_loss);
                            balances.push(current_balance + order_result.amount);
                            positions.push(0);
                            actions.push(order_result.signal.get_column().to_string());
                            current_order = None;
                            continue;
                        }
                        Err(_) => {}
                    }
                }
                // TRANSACTION modifiers (stop loss, take profit) should be checked for closing positions regardless of signals
                // this is the place to implement it

                // position is not closed
                let is_position_close = contains_position_close
                    && signals_cols_map.get("position_close").unwrap()[index] == 1;
                let is_long_close = current_position == 1
                    && contains_long_close
                    && signals_cols_map.get("long_close").unwrap()[index] == 1;
                let is_short_close = current_position == -1
                    && contains_short_close
                    && signals_cols_map.get("short_close").unwrap()[index] == 1;
                if is_position_close || is_long_close || is_short_close {
                    let current_price = additional_cols_map.get(&price_col).unwrap()[index];

                    let close_signal = if is_position_close {
                        SignalCategory::ClosePosition
                    } else if is_short_close {
                        SignalCategory::CloseShort
                    } else if is_long_close {
                        SignalCategory::CloseLong
                    } else {
                        SignalCategory::KeepPosition
                    };

                    match order.close_order(current_price, position_modifier.clone(), close_signal)
                    {
                        Ok(order_result) => {
                            trade_fees.push(order_result.close_fee);
                            units.push(0.0);
                            profit_and_loss.push(order_result.profit_and_loss);
                            balances.push(current_balance + order_result.amount);
                            positions.push(0);
                            actions.push(order_result.signal.get_column().to_string());
                            current_order = None;
                            continue;
                        }
                        Err(e) => {
                            println!("ORDER CLOSE WARNING: {:?}", e)
                        }
                    }
                }
            }

            trade_fees.push(0.0);
            units.push(current_units);
            profit_and_loss.push(0.0);
            positions.push(current_position);
            actions.push(SignalCategory::KeepPosition.get_column().to_string());
            balances.push(current_balance);
        }
        // if last position was taken
        if positions.last().unwrap() != &0 {
            if let Some((before_last_order_index, _)) =
                positions // over positions vector
                    .iter() // iterate over
                    .enumerate() // an enumeration
                    .rev() // of reversed positions
                    .find(|(_, value)| value == &&0)
            // until it finds where value is 0
            {
                // splices results vectors to values before opening the order

                // note that even though the vector was reversed, before_last_order_index keeps being the original vector index. Thanks, Rust <3
                let range = before_last_order_index..dataframe_height;
                let zeroed_float_patch: Vec<f64> = range.clone().map(|_| 0.0 as f64).collect();
                let zeroed_integer_patch: Vec<i32> = range.clone().map(|_| 0 as i32).collect();
                let keep_position_action_patch: Vec<String> = range
                    .clone()
                    .map(|_| SignalCategory::KeepPosition.get_column().to_string())
                    .collect();

                trade_fees.splice(range.clone(), zeroed_float_patch.clone());
                units.splice(range.clone(), zeroed_float_patch.clone());
                profit_and_loss.splice(range.clone(), zeroed_float_patch.clone());

                positions.splice(range.clone(), zeroed_integer_patch);
                actions.splice(range.clone(), keep_position_action_patch);

                let previous_balance = balances[before_last_order_index];
                let patch_balances: Vec<f64> =
                    range.clone().map(|_| previous_balance as f64).collect();
                balances.splice(range.clone(), patch_balances);
                returns.splice(range.clone(), zeroed_float_patch);
            }
        }

        let elapsed_time = start_time.elapsed();
        let elapsed_millis = elapsed_time.as_nanos();
        println!(
            "compute_benchmark_positions => Elapsed time in nanos: {}",
            elapsed_millis
        );

        let trade_fee_series = Series::new("trade_fees", trade_fees);
        let units_series = Series::new("units", units);
        let profit_and_loss_series = Series::new("P&L", profit_and_loss);
        let returns_series = Series::new("returns", returns);
        let balance_series = Series::new("balance", balances);
        let position_series = Series::new("position", positions);
        let action_series = Series::new("action", actions);

        let df = df.with_column(trade_fee_series)?;
        let df = df.with_column(units_series)?;
        let df = df.with_column(profit_and_loss_series)?;
        let df = df.with_column(returns_series)?;
        let df = df.with_column(balance_series)?;
        let df = df.with_column(position_series)?;
        let df = df.with_column(action_series)?;

        let result = df.clone().lazy().select([
            col("start_time"),
            col("trade_fees"),
            col("units"),
            col("P&L"),
            col("returns"),
            col("balance"),
            col("position"),
            col("action"),
        ]);

        Ok(result)
    }

    // pub fn update_positions(&self, current_trading_data: DataFrame, last_period_tick_data: DataFrame) -> Result<DataFrame, Error> {
    //     let trading_data = current_trading_data.vstack(&last_period_tick_data)?;
    //     let strategy_data = self.update_strategy_data(&trading_data)?;

    //     // trading_data = self.update_trading_data(&trading_data)?;
    //     // let path = "data/test".to_string();
    //     // let file_name = "updated.csv".to_string();
    //     // save_csv(path.clone(), file_name, &trading_data, true)?;

    //     // let signal = self.generate_last_position_signal(&trading_data)?;
    //     // self.trading_data.next(trading_data);

    //     // self.signal_generator.next(Some(signal));

    //     Ok(strategy_data)
    // }

    pub fn update_strategy_data(
        &self,
        current_trading_data: DataFrame,
        last_period_tick_data: DataFrame,
    ) -> Result<DataFrame, Error> {
        let mut strategy_data = current_trading_data.vstack(&last_period_tick_data)?;

        // let mut df = data.sort(["start_time"], vec![false])?;
        strategy_data = self.update_preindicators_data(strategy_data)?;
        strategy_data = self.update_indicators_data(strategy_data)?;
        strategy_data = self.update_signals_data(strategy_data)?;
        Ok(strategy_data)
    }

    fn update_preindicators_data(&self, data: DataFrame) -> Result<DataFrame, Error> {
        let mut data = data.to_owned();
        for pre_indicator in &self.pre_indicators {
            data = pre_indicator.update_indicator_columns(&data)?;
        }
        Ok(data)
    }

    fn update_indicators_data(&self, data: DataFrame) -> Result<DataFrame, Error> {
        let mut data = data.to_owned();
        for indicator in &self.indicators {
            data = indicator.update_indicator_columns(&data)?;
        }
        Ok(data)
    }

    fn update_signals_data(&self, data: DataFrame) -> Result<DataFrame, Error> {
        let mut data = data.to_owned();
        for signal in &self.signals {
            data = signal.update_signal_column(&data)?;
        }
        Ok(data)
    }

    pub fn generate_last_position_signal(&self, data: &DataFrame) -> Result<SignalCategory, Error> {
        // uses hashset to ensure no SignalCategory is double counted
        let mut signals_cols = HashSet::new();
        for signal in self.signals.clone().into_iter() {
            signals_cols.insert(String::from(signal.signal_category().get_column()));
        }

        let contains_short = signals_cols.contains(SignalCategory::GoShort.get_column());

        let contains_long = signals_cols.contains(SignalCategory::GoLong.get_column());

        let contains_short_close = signals_cols.contains(SignalCategory::CloseShort.get_column());

        let contains_long_close = signals_cols.contains(SignalCategory::CloseLong.get_column());

        let contains_position_close =
            signals_cols.contains(SignalCategory::ClosePosition.get_column());

        let signals_filtered_df = data.select(&signals_cols)?;

        let mut signals_cols_map = HashMap::new();

        signals_cols.into_iter().for_each(|col| {
            signals_cols_map.insert(
                col.clone(),
                signals_filtered_df
                    .column(&col)
                    .unwrap()
                    .i32()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<i32>>(),
            );
        });
        let binding = self.exchange_listener.ref_value();
        let traded_symbol = &binding.get_traded_contract().symbol;
        let (_, high_col, low_col, price_col) = get_symbol_ohlc_cols(traded_symbol);
        let additional_cols = vec![price_col.clone(), high_col.clone(), low_col.clone()];

        let additional_filtered_df = data.select(&additional_cols)?;
        let mut additional_cols_map = HashMap::new();
        additional_cols.into_iter().for_each(|col| {
            additional_cols_map.insert(
                col.clone(),
                additional_filtered_df
                    .column(&col)
                    .unwrap()
                    .f64()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<f64>>(),
            );
        });

        let last_index = data.height() - 1;
        // let penultimate_index = last_index - 1;

        let positions = data
            .column("position")?
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        let current_position = positions[last_index].unwrap();

        // position is neutral
        if current_position == 0 {
            if contains_short
                && signals_cols_map
                    .get(SignalCategory::GoShort.get_column())
                    .unwrap()[last_index]
                    == 1
            {
                return Ok(SignalCategory::GoShort);
            }

            if contains_long
                && signals_cols_map
                    .get(SignalCategory::GoLong.get_column())
                    .unwrap()[last_index]
                    == 1
            {
                return Ok(SignalCategory::GoLong);
            }
        } else {
            // position is not closed
            let is_position_close = contains_position_close
                && signals_cols_map.get("position_close").unwrap()[last_index] == 1;
            let is_long_close = current_position == 1
                && contains_long_close
                && signals_cols_map.get("long_close").unwrap()[last_index] == 1;
            let is_short_close = current_position == -1
                && contains_short_close
                && signals_cols_map.get("short_close").unwrap()[last_index] == 1;
            if is_position_close || is_long_close || is_short_close {
                let close_signal = if is_position_close {
                    SignalCategory::ClosePosition
                } else if is_short_close {
                    SignalCategory::CloseShort
                } else if is_long_close {
                    SignalCategory::CloseLong
                } else {
                    return Ok(SignalCategory::KeepPosition);
                };

                return Ok(close_signal);
            }
        }

        Ok(SignalCategory::KeepPosition)
    }
}

fn take_position_by_balance<'a>(
    contract: &'a Contract,
    position: i32,
    price: f64,
    balance: f64,
    fee_rate: f64,
    leverage_factor: f64,
) -> LeveragedOrder {
    LeveragedOrder::new(
        contract,
        position,
        None,
        Some(balance),
        price,
        leverage_factor,
        fee_rate,
    )
}

#[derive(Clone, Debug)]
pub struct LeveragedOrder {
    contract: Contract,
    status: OrderStatus,
    pub position: i32,
    pub units: f64, // non-levered
    pub price: f64,
    pub bankruptcy_price: f64,
    pub leverage_factor: f64,
    pub fee_rate: f64,
    pub open_fee: f64,
    pub close_fee: f64,
    pub balance_remainder: f64,
}

impl LeveragedOrder {
    pub fn new(
        contract: &Contract,
        position: i32,
        units_opt: Option<f64>,
        balance_opt: Option<f64>,
        price: f64,
        leverage_factor: f64,
        fee_rate: f64,
    ) -> Self {
        let mut units = if let Some(_units) = units_opt {
            _units
        } else if let Some(_balance) = balance_opt {
            // Order Cost = Initial Margin + Fee to Open Position + Fee to Close Position
            // Initial Margin = (Order Price × Order Quantity) / Leverage
            // Fee to Open Position = Order Quantity × Order Price × Taker Fee Rate
            // Fee to Close Position = Order Quantity × Bankruptcy Price × Taker Fee Rate
            // Order Cost × Leverage / [Order Price × (0.0012 × Leverage + 1.0006)]

            // _balance / ((price / leverage_factor) + (price * fee_rate) + (default_price * fee_rate))
            let position_mod = if position == -1 {
                fee_rate
            } else if position == 1 {
                -fee_rate
            } else {
                0.0
            };
            _balance * leverage_factor
                / (price * (((2.0 * fee_rate) * leverage_factor) + (1.0 + position_mod)))
        } else {
            0.0
        };

        let fract_units = calculate_remainder(units, contract.minimum_order_size);

        units -= fract_units;

        if units == 0.0
            || units < contract.minimum_order_size
            || units > contract.maximum_order_size
            || leverage_factor > contract.max_leverage
        {
            println!("Some contract constraints stopped the order from being placed");
            if units == 0.0 {
                println!("units == 0.0 -> {}", units == 0.0)
            };
            if units < contract.minimum_order_size {
                println!(
                    "units < contract.minimum_order_size -> {}",
                    units < contract.minimum_order_size
                );
            }
            if units > contract.maximum_order_size {
                println!(
                    "units > contract.maximum_order_size -> {}",
                    units > contract.maximum_order_size
                );
            }
            if leverage_factor > contract.max_leverage {
                println!(
                    "leverage_factor > contract.max_leverage -> {}",
                    leverage_factor > contract.max_leverage
                );
            }
            return LeveragedOrder {
                contract: contract.clone(),
                status: OrderStatus::Cancelled,
                units: 0.0,
                price,
                bankruptcy_price: 0.0,
                leverage_factor,
                fee_rate,
                position: 0,
                open_fee: 0.0,
                close_fee: 0.0,
                balance_remainder: 0.0,
            };
        }
        let balance_remainder = fract_units * price / leverage_factor;
        let open_fee = units * price * fee_rate;
        let bankruptcy_price = if position == -1 {
            // Bankruptcy Price for Short Position = Order Price × ( Leverage + 1) / Leverage
            price * (leverage_factor + 1.0) / leverage_factor
        } else if position == 1 {
            // Bankruptcy Price for Long Position = Order Price × ( Leverage − 1) / Leverage
            price * (leverage_factor - 1.0) / leverage_factor
        } else {
            0.0
        };
        let close_fee = units * bankruptcy_price * fee_rate;

        LeveragedOrder {
            contract: contract.clone(),
            status: OrderStatus::Filled,
            units,
            price,
            bankruptcy_price,
            leverage_factor,
            fee_rate,
            position,
            open_fee,
            close_fee,
            balance_remainder,
        }
    }

    pub fn get_order_value(&self) -> f64 {
        self.units * self.price
    }

    pub fn get_order_cost(&self, close_price_opt: Option<f64>) -> f64 {
        let im = self.get_order_value() / self.leverage_factor;
        if let Some(close_price) = close_price_opt {
            im + self.open_fee + (self.units * close_price * self.fee_rate)
        } else {
            im + self.open_fee + self.close_fee
        }
    }

    pub fn get_close_data(
        &self,
        close_price: f64,
    ) -> Result<(f64, f64, f64, f64, f64), OrderCloseError> {
        let revenue = if self.position == -1 {
            self.units * (self.price - close_price)
        } else if self.position == 1 {
            self.units * (close_price - self.price)
        } else {
            return Err(OrderCloseError::InvalidInitialPosition);
        };
        // Closed P&L = Position P&L - Fee to open - Fee to close - Sum of all funding fees paid/received
        // Fee to open = Qty x Entry price x 0.06% = 1.44 USDT paid out
        // Fee to close = Qty x Exit price x 0.06% = 1.2 USDT paid out
        // Sum of all funding fees paid/received = 2.10 USDT paid out
        let close_fee = self.units * close_price * self.fee_rate;
        let profit_and_loss = revenue - self.open_fee - close_fee;
        let order_cost = self.get_order_cost(Some(close_price));
        let amount = f64::max(0.0, order_cost + profit_and_loss);
        let returns = (amount / order_cost) - 1.0;
        Ok((revenue, close_fee, profit_and_loss, amount, returns))
    }

    pub fn close_order(
        &mut self,
        close_price: f64,
        lock: PositionLock,
        signal: SignalCategory,
    ) -> Result<OrderClose, OrderCloseError> {
        let (revenue, close_fee, profit_and_loss, amount, returns) =
            self.get_close_data(close_price).unwrap();

        match lock {
            PositionLock::Nil => {
                self.close_fee = close_fee;
                Ok(OrderClose {
                    close_fee,
                    returns,
                    profit_and_loss,
                    amount,
                    signal,
                })
            }
            PositionLock::Fee => {
                if self.open_fee + close_fee >= revenue.abs() {
                    // keeps position
                    Err(OrderCloseError::FeeLock)
                } else {
                    self.close_fee = close_fee;
                    Ok(OrderClose {
                        close_fee,
                        returns,
                        profit_and_loss,
                        amount,
                        signal,
                    })
                }
            }
            PositionLock::Loss => {
                if profit_and_loss <= 0.0 {
                    Err(OrderCloseError::LossLock)
                } else {
                    self.close_fee = close_fee;
                    Ok(OrderClose {
                        close_fee,
                        returns,
                        profit_and_loss,
                        amount,
                        signal,
                    })
                }
            }
        }
    }

    pub fn check_price_level_modifiers(
        &mut self,
        min_price: f64,
        max_price: f64,
        current_price: f64,
        stop_loss_opt: Option<&PriceLevel>,
        take_profit_opt: Option<&PriceLevel>,
    ) -> Result<OrderClose, OrderCloseError> {
        let ref_price: f64 = if self.position == -1 {
            max_price
        } else if self.position == 1 {
            min_price
        } else {
            return Err(OrderCloseError::InvalidInitialPosition);
        };

        if self.leverage_factor > 1.0 {
            if self.position == -1 && self.bankruptcy_price <= ref_price {
                return self.close_order(
                    ref_price,
                    PositionLock::Nil,
                    SignalCategory::LeverageBankrupcty,
                );
            }
            if self.position == 1 && self.bankruptcy_price >= ref_price {
                return self.close_order(
                    ref_price,
                    PositionLock::Nil,
                    SignalCategory::LeverageBankrupcty,
                );
            }
        }
        if stop_loss_opt.is_some() || take_profit_opt.is_some() {
            let (_, _, _, _, returns) = self.get_close_data(ref_price).unwrap();
            if let Some(stop_loss) = stop_loss_opt {
                let stop_loss_percentage = stop_loss.get_percentage();
                if returns < 0.0 && stop_loss_percentage <= returns.abs() {
                    println!(
                        "Stop loss took effect: returns = {}, stop loss percentage = {}",
                        returns, stop_loss_percentage
                    );
                    return self.close_order(
                        ref_price,
                        PositionLock::Nil,
                        SignalCategory::StopLoss,
                    );
                }
            }

            if let Some(take_profit) = take_profit_opt {
                let take_profit_percentage = take_profit.get_percentage();
                if returns > 0.0 && take_profit_percentage <= returns.abs() {
                    println!(
                        "Take profit took effect: returns = {}, take profit percentage = {}",
                        returns, take_profit_percentage
                    );
                    return self.close_order(
                        current_price,
                        PositionLock::Nil,
                        SignalCategory::TakeProfit,
                    );
                }
            }
        }

        return Err(OrderCloseError::Nil);
    }
}

#[derive(Debug)]
pub struct OrderClose {
    pub close_fee: f64,
    pub profit_and_loss: f64,
    pub returns: f64,
    pub amount: f64,
    pub signal: SignalCategory,
}
#[derive(Debug)]
pub enum OrderCloseError {
    Nil,
    InvalidInitialPosition,
    FeeLock,
    LossLock,
}
