use crate::{
    shared::csv::save_csv,
    trader::functions::{get_symbol_ohlc_cols, map_tick_data_to_data_lf},
};

use super::{
    super::constants::SECONDS_IN_DAY,
    super::errors::Error,
    behavior_subject::BehaviorSubject,
    contract::Contract,
    exchange::Exchange,
    indicator::Indicator,
    modifiers::{Leverage, PositionLockModifier, PriceLevelModifier},
    performance::Performance,
    signal::{SignalCategory, Signer},
    TickData,
};
use chrono::{Duration, NaiveDateTime};
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub struct Strategy {
    pub name: String,
    pub pre_indicators: Vec<Box<dyn Indicator + Send + Sync>>,
    pub indicators: Vec<Box<dyn Indicator + Send + Sync>>,
    pub signals: Vec<Box<dyn Signer + Send + Sync>>,
    pub data: BehaviorSubject<DataFrame>,
    pub initial_balance: f64,
    pub exchange: Exchange,
    pub leverage: Leverage,
    pub position_modifier: PositionLockModifier,
    pub price_level_modifiers: HashMap<String, PriceLevelModifier>,
    pub staging_area: StagingArea,
    tick_data_vec: Vec<TickData>,
    performance_arc: Arc<Mutex<Performance>>,
}

pub struct StagingArea {
    pub current_order: Option<LeveredOrder>,
    pub current_balance: f64,
}

impl StagingArea {
    pub fn new(initial_balance: f64) -> Self {
        StagingArea {
            current_order: None,
            current_balance: initial_balance,
        }
    }
}

impl Default for StagingArea {
    fn default() -> Self {
        StagingArea {
            current_order: None,
            current_balance: 0.0,
        }
    }
}

impl Clone for Strategy {
    fn clone(&self) -> Self {
        Self::new(
            self.name.clone(),
            self.pre_indicators.clone(),
            self.indicators.clone(),
            self.signals.clone(),
            self.data.clone(),
            self.initial_balance,
            self.exchange.clone(),
            Some(self.leverage.clone()),
            self.position_modifier.clone(),
            self.price_level_modifiers.clone(),
            self.performance_arc.clone(),
        )
    }
}

impl Strategy {
    pub fn new(
        name: String,
        pre_indicators: Vec<Box<dyn Indicator + Send + Sync>>,
        indicators: Vec<Box<dyn Indicator + Send + Sync>>,
        signals: Vec<Box<dyn Signer + Send + Sync>>,
        data: BehaviorSubject<DataFrame>,
        initial_balance: f64,
        exchange: Exchange,
        leverage_opt: Option<Leverage>,
        position_modifier: PositionLockModifier,
        price_level_modifiers: HashMap<String, PriceLevelModifier>,
        performance_arc: Arc<Mutex<Performance>>,
    ) -> Strategy {
        let mut leverage = Leverage::default();
        if let Some(lambda) = leverage_opt {
            leverage = lambda;
        }
        Strategy {
            name,
            pre_indicators,
            indicators,
            signals,
            data,
            initial_balance,
            exchange,
            leverage,
            position_modifier,
            price_level_modifiers,
            staging_area: StagingArea::new(initial_balance),
            tick_data_vec: vec![],
            performance_arc,
        }
    }

    pub fn set_benchmark(
        &mut self,
        tick_data: LazyFrame,
        last_bar: &NaiveDateTime,
        traded_contract: &Contract,
    ) -> Result<(), Error> {
        println!("SET BENCHMARK");
        let tick_data = tick_data.cache();
        let mut lf = self.set_strategy_data(tick_data)?;

        let benchmark_positions = self.compute_benchmark_positions(&lf, traded_contract)?;
        lf = lf.left_join(benchmark_positions, "start_time", "start_time");

        // get only signals from previous day
        let minus_one_day_timestamp = last_bar.timestamp() * 1000 - SECONDS_IN_DAY * 1000;

        // save benchmark before filtering last day in order to understand better stochastic indicator
        // let path = "data/test".to_string();
        // let file_name = "benchmark_full.csv".to_string();
        // let df = lf.clone().collect()?;
        // save_csv(path.clone(), file_name, &df, true)?;

        lf = lf.filter(
            col("start_time")
                .dt()
                .timestamp(TimeUnit::Milliseconds)
                .gt_eq(minus_one_day_timestamp),
        );

        // TODO: subscribe to strategy data behavior subject and calculate performance based on strategy data changes
        let mut performance = self.performance_arc.lock().unwrap();
        performance.set_benchmark(&lf, &traded_contract.symbol)?;
        lf = lf.cache();
        self.data.next(lf.collect()?);

        Ok(())
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

    fn compute_benchmark_positions(
        &self,
        data: &LazyFrame,
        traded_contract: &Contract,
    ) -> Result<LazyFrame, Error> {
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
        let mut current_order: Option<LeveredOrder> = None;

        // fee = balance * leverage * price * fee_rate
        // marginal fee = price * fee_rate
        let mut trade_fees = vec![0.0];
        let mut units = vec![0.0];
        let mut profit_and_loss = vec![0.0];
        let mut returns = vec![0.0];
        let mut balances = vec![self.initial_balance];
        let mut positions = vec![0];
        let mut actions = vec![SignalCategory::KeepPosition.get_column().to_string()];

        let leverage_factor = self.leverage.get_factor();
        let has_leverage = if leverage_factor > 1.0 { true } else { false };
        let stop_loss_opt: Option<&PriceLevelModifier> = self
            .price_level_modifiers
            .get(&PriceLevelModifier::StopLoss(0.0).get_hash_key());
        let take_profit_opt = self
            .price_level_modifiers
            .get(&PriceLevelModifier::TakeProfit(0.0).get_hash_key());
        let dataframe_height = df.height();
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
                        self.exchange.taker_fee_rate,
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
                        self.exchange.taker_fee_rate,
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

                    match order.close_order(current_price, self.position_modifier, close_signal) {
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

                let range = before_last_order_index..dataframe_height; // note that even though the vector was reversed, before_last_order_index keeps being the original vector index. Thanks, Rust <3stoploss
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

    fn update_positions(
        &mut self,
        data: DataFrame,
        traded_contract: &Contract,
        subject: BehaviorSubject<Option<SignalCategory>>,
    ) -> Result<(), Error> {
        println!("UPDATE POSITIONS");
        let df = self.update_strategy_data(data)?;
        let path = "data/test".to_string();
        let file_name = "updated.csv".to_string();
        save_csv(path.clone(), file_name, &df, true)?;
        let signal = self.generate_last_position_signal(&df, traded_contract)?;
        subject.next(signal);

        // TODO: This should happen after dispatching order / receiving result
        self.data.next(df);

        Ok(())
    }

    fn update_strategy_data(&self, data: DataFrame) -> Result<DataFrame, Error> {
        let mut df = data.sort(["start_time"], vec![false])?;
        df = self.update_preindicators_data(df)?;
        df = self.update_indicators_data(df)?;
        df = self.update_signals_data(df)?;
        Ok(df)
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

    fn generate_last_position_signal(
        &self,
        data: &DataFrame,
        traded_contract: &Contract,
    ) -> Result<Option<SignalCategory>, Error> {
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

        let (_, high_col, low_col, price_col) = get_symbol_ohlc_cols(&traded_contract.symbol);
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
        let penultimate_index = last_index - 1;

        let positions = data
            .column("position")?
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(_) = positions[last_index] {
            // last position was already filled by stop loss / take profit, return None
            return Ok(None);
        } else {
            // no position has been taken yet, so fetch latest signals
            let current_position = positions[penultimate_index].unwrap();
            // CHECK THIS:
            let is_staging_area_order_empty = self.staging_area.current_order.is_none();

            // position is neutral
            if current_position == 0 && is_staging_area_order_empty {
                if contains_short && signals_cols_map.get("short").unwrap()[last_index] == 1 {
                    return Ok(Some(SignalCategory::GoShort));
                }

                if contains_long && signals_cols_map.get("long").unwrap()[last_index] == 1 {
                    return Ok(Some(SignalCategory::GoShort));
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
                        return Ok(None);
                    };

                    return Ok(Some(close_signal));
                }
            }
        }

        Ok(Some(SignalCategory::KeepPosition))
    }

    pub fn process_tick_data(
        &mut self,
        tick_data: TickData,
        last_bar: &NaiveDateTime,
        bar_length: Duration,
        contracts: &[Contract; 2],
        subject: BehaviorSubject<Option<SignalCategory>>,
    ) -> Result<NaiveDateTime, Error> {
        // checks if close price reached stop_loss or take_profit prices
        // adds tick_data to tick_data_vec
        // checks if last tick_data minus last_bar is greater or equals bar_length

        let last_tick_datetime = tick_data.date;
        self.tick_data_vec.push(tick_data.clone());

        if tick_data.date - *last_bar >= bar_length {
            // let start_time = Instant::now();

            let last_period_tick_data_df = map_tick_data_to_data_lf(
                &contracts.clone().map(|c| c.symbol),
                self.tick_data_vec.clone(),
                &self.data.value().schema(),
                &last_bar,
                &bar_length,
            )?;
            let updated_tick_data_df = self.data.value().vstack(&last_period_tick_data_df)?;

            // let filter_datetime = *last_bar - Duration::days(1);
            // let last_day_filter_mask = updated_tick_data_df
            //     .column("start_time")?
            //     .gt(filter_datetime.timestamp_millis())?;
            // updated_tick_data_df = updated_tick_data_df.filter(&last_day_filter_mask)?;

            let _ = self.update_positions(updated_tick_data_df, &contracts[1], subject);

            // let elapsed_time = start_time.elapsed();
            // let elapsed_millis = elapsed_time.as_nanos();
            // println!(
            //     "process_tick_data => Elapsed time in nanos: {}",
            //     elapsed_millis
            // );

            // println!("@@@ updated_tick_data_df LFS {:?}", updated_tick_data_df);

            // tick_data_lf = concat_and_clean_lazyframes([tick_data_lf, new_tick_data_lf], filter_datetime)?;
            self.tick_data_vec.clear();
            return Ok(last_tick_datetime);
            // tick_data_payload.clear();
        }
        Ok(*last_bar)
        // if so, consolidade tick_data into lazyframe and append indicators
        // after that, clear tick_data_lf
    }
}

fn take_position_by_balance<'a>(
    contract: &'a Contract,
    position: i32,
    price: f64,
    balance: f64,
    fee_rate: f64,
    leverage_factor: f64,
) -> LeveredOrder {
    LeveredOrder::new(
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
pub struct LeveredOrder {
    contract: Contract,
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

unsafe impl Send for LeveredOrder {}
unsafe impl Sync for LeveredOrder {}

impl LeveredOrder {
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
            return LeveredOrder {
                contract: contract.clone(),
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

        LeveredOrder {
            contract: contract.clone(),
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
        lock: PositionLockModifier,
        signal: SignalCategory,
    ) -> Result<OrderClose, OrderCloseError> {
        let (revenue, close_fee, profit_and_loss, amount, returns) =
            self.get_close_data(close_price).unwrap();

        match lock {
            PositionLockModifier::Nil => {
                self.close_fee = close_fee;
                Ok(OrderClose {
                    close_fee,
                    returns,
                    profit_and_loss,
                    amount,
                    signal,
                })
            }
            PositionLockModifier::Fee => {
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
            PositionLockModifier::Loss => {
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
        stop_loss_opt: Option<&PriceLevelModifier>,
        take_profit_opt: Option<&PriceLevelModifier>,
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
                    PositionLockModifier::Nil,
                    SignalCategory::LeverageBankrupcty,
                );
            }
            if self.position == 1 && self.bankruptcy_price >= ref_price {
                return self.close_order(
                    ref_price,
                    PositionLockModifier::Nil,
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
                        PositionLockModifier::Nil,
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
                        PositionLockModifier::Nil,
                        SignalCategory::TakeProfit,
                    );
                }
            }
        }

        return Err(OrderCloseError::Nil);
    }
}

fn calculate_remainder(dividend: f64, divisor: f64) -> f64 {
    dividend - (dividend / divisor).floor() * divisor
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
