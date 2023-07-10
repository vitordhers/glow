use crate::{shared::csv::save_csv, trader::functions::get_symbol_ohlc_cols};

use super::{
    super::constants::SECONDS_IN_DAY,
    super::errors::Error,
    async_behavior_subject::AsyncBehaviorSubject,
    contract::Contract,
    exchange::Exchange,
    // super::functions::print_as_df,
    indicator::Indicator,
    modifiers::{Leverage, PositionLockModifier, PriceLevelModifier},
    performance::Performance,
    // market_data::MarketDataFeedDTE,
    signal::{SignalCategory, Signer},
};
use chrono::Duration as ChronoDuration;
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

// use std::borrow::BorrowMut;
// use async_trait::async_trait;
// use tokio::sync::watch::error::SendError;

// #[derive(Clone)]
pub struct Strategy {
    pub name: String,
    pub pre_indicator_cols: Option<Box<dyn Indicator + Send + Sync>>,
    pub indicators: Vec<Box<dyn Indicator + Send + Sync>>,
    pub signals: Vec<Box<dyn Signer + Send + Sync>>,
    pub data: AsyncBehaviorSubject<LazyFrame>,
    pub initial_balance: f64,
    pub exchange: Exchange,
    pub leverage: Leverage,
    pub position_modifier: PositionLockModifier,
    pub price_level_modifiers: HashMap<String, PriceLevelModifier>,
    // pub receivers: Vec<StrategyReceiver>,
    // pub sender: Sender<StrategyDTE>,
}
impl Clone for Strategy {
    fn clone(&self) -> Self {
        Self::new(
            self.name.clone(),
            self.pre_indicator_cols.clone(),
            self.indicators.clone(),
            self.signals.clone(),
            self.initial_balance,
            self.exchange.clone(),
            Some(self.leverage.clone()),
            self.position_modifier.clone(),
            self.price_level_modifiers.clone(),
        )
    }
}

impl Strategy {
    pub fn new(
        name: String,
        pre_indicator_cols: Option<Box<dyn Indicator + Send + Sync>>,
        indicators: Vec<Box<dyn Indicator + Send + Sync>>,
        signals: Vec<Box<dyn Signer + Send + Sync>>,
        initial_balance: f64,
        exchange: Exchange,
        leverage_opt: Option<Leverage>,
        position_modifier: PositionLockModifier,
        price_level_modifiers: HashMap<String, PriceLevelModifier>, // sender: Sender<StrategyDTE>,
                                                                    // market_data_feed_receiver: Receiver<MarketDataFeedDTE>,
    ) -> Strategy {
        let mut leverage = Leverage::default();
        if let Some(lambda) = leverage_opt {
            leverage = lambda;
        }
        Strategy {
            name,
            pre_indicator_cols,
            indicators,
            signals,
            data: AsyncBehaviorSubject::new(LazyFrame::default()),
            initial_balance,
            exchange,
            leverage,
            position_modifier,
            price_level_modifiers, // sender,
                                   // receivers: vec![StrategyReceiver::MarketDataFeed(market_data_feed_receiver)],
        }
    }

    // pub async fn listen_events(self) -> Result<Result<(), Error>, JoinError> {
    //     let observed = self.data_feed_arc;
    //     let handle = tokio::spawn(async move {
    //         let lf = wait_for_update(&*observed).await;
    //         let d = NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
    //         let t = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap();
    //         self.set_benchmark(lf.0, &NaiveDateTime::new(d, t), &"AGIX_USDT".to_string())
    //     });
    //     handle.await
    // }

    pub fn set_benchmark(
        &mut self,
        performance: &mut Performance,
        tick_data: LazyFrame,
        last_bar: &NaiveDateTime,
        traded_contract: &Contract,
    ) -> Result<(), Error> {
        println!("SET BENCHMARK");
        let tick_data = tick_data.cache();
        let mut lf = self.set_benchmark_data(tick_data)?;

        let benchmark_positions = self.compute_benchmark_positions(&lf, traded_contract)?;
        lf = lf.left_join(benchmark_positions, "start_time", "start_time");

        // get only signals from previous day
        let minus_one_day_timestamp = last_bar.timestamp() * 1000 - SECONDS_IN_DAY * 1000;

        lf = lf.filter(
            col("start_time")
                .dt()
                .timestamp(TimeUnit::Milliseconds)
                .gt_eq(minus_one_day_timestamp),
        );

        self.data.next(lf.clone());
        performance.set_benchmark(&lf, &traded_contract.symbol)?;

        Ok(())
    }

    pub fn set_benchmark_data(&self, tick_data: LazyFrame) -> Result<LazyFrame, Error> {
        let mut lf = self.set_benchmark_pre_indicators(tick_data)?;
        lf = self.set_benchmark_indicators(&lf)?;
        lf = self.set_signals(&lf)?;
        Ok(lf)
    }

    fn set_benchmark_pre_indicators(&self, tick_data: LazyFrame) -> Result<LazyFrame, Error> {
        match &self.pre_indicator_cols {
            Some(boxed_fn) => {
                let preindicator = boxed_fn.as_ref();
                // print_names(&tick_data, "PRE INDICATORS TICK DATA".into())?;
                let new_data = preindicator.compute_indicator_columns(tick_data.clone())?;
                // print_names(&new_data, "PRE INDICATORS NEW DATA".into())?;

                let lf = tick_data.left_join(new_data, "start_time", "start_time");
                // print_names(&lf, "PRE INDICATORS LF DATA".into())?;

                Ok(lf)
            }
            None => Ok(tick_data),
        }
    }

    fn set_benchmark_indicators(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();

        for indicator in &self.indicators {
            let lf = indicator.compute_indicator_columns(data.clone())?;
            data = data.left_join(lf, "start_time", "start_time");
        }

        Ok(data)
    }

    fn set_signals(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();
        for signal in &self.signals {
            let lf = signal.compute_signal_column(&data)?;
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
        println!("Elapsed time in nanos: {}", elapsed_millis);

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

    // fn get_signals_categories_cols(&self) -> Vec<String> {
    //     let mut categories_set: HashSet<String> = HashSet::new();

    //     for signal in &self.signals {
    //         let category = signal.signal_category();
    //         let cat = category.get_column();
    //         categories_set.insert(cat);
    //     }
    //     categories_set.into_iter().collect()
    // }

    #[allow(dead_code)]
    pub fn update_positions(&mut self, _tick_data: &LazyFrame) -> Result<(), Error> {
        Ok(())
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
pub struct LeveredOrder<'a> {
    contract: &'a Contract,
    pub position: i32,
    pub units: f64, // non-leveraged
    pub price: f64,
    pub bankruptcy_price: f64,
    pub leverage_factor: f64,
    pub fee_rate: f64,
    pub open_fee: f64,
    pub close_fee: f64,
    pub balance_remainder: f64,
}

impl<'a> LeveredOrder<'a> {
    pub fn new(
        contract: &'a Contract,
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
                contract,
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
            contract,
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

#[test]
fn test_stop_loss_threshold() {
    // units = 10
    let balance = 0.0182;
    let price = 0.222;
    let AGIXUSDT: Contract = Contract::new(
        String::from("AGIXUSDT"),
        0.000073,
        ChronoDuration::hours(8),
        None,
        0.00005,
        300000.0,
        1.0,
        25.0,
    );

    let ARBUSDT: Contract = Contract::new(
        String::from("ARBUSDT"),
        0.0001,
        ChronoDuration::hours(8),
        None,
        0.0001,
        180000.0,
        0.1,
        50.0,
    );
    let leverage = 25.0;
    let fee_rate = 0.0006;
    let position = -1;
    let order = LeveredOrder::new(
        &AGIXUSDT,
        position,
        None,
        Some(balance),
        price,
        leverage,
        fee_rate,
    );
    let cost = 0.0089;
    let value = 0.222;

    let order_cost = order.get_order_cost(None);
    let order_value = order.get_order_value();

    // let stop_loss_roi = 10.0;

    // let stop_loss_price = calculate_stop_loss_price(
    //     10.0,
    //     balance,
    //     price,
    //     stop_loss_roi,
    //     leverage,
    //     position,
    //     fee_rate,
    // );

    // Define your expected result here based on the input values
    // let expected_stop_loss_price = 0.23005;

    println!("ORDER COST {} AND VALUE {}", order_cost, order_value);
    // println!("EXPECTED PRICE TIMES QTY2 {}", stop_loss_price * 10.0);

    // Assert that the calculated stop loss price matches the expected result
    assert_eq!(cost, order_cost);
    assert_eq!(value, order_value);
}

fn round_down_nth_decimal(num: f64, n: i32) -> f64 {
    let multiplier = 10.0_f64.powi(n);
    (num * multiplier).floor() / multiplier
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

// fn calculate_stop_loss_price(
//     units_or_balance: LeveredOrder,
//     start_price: f64,
//     stop_loss_roi: f64,
//     leverage_factor: f64,
//     position: i32,
//     fee_rate: f64,
// ) -> f64 {
//     let units = 8.8; // w leverage
//     let price = 1.1363;
//     // start_order_value = units / start_price;
//     // start_order_value = 8.8 / 1.1363
//     // start_order_cost = start_order_value * fee_rate
//     // 0.00599967 = 8.8 / 1.1363 * 0.0006

//     let start_order_value = units / start_price;
//     // start_order_value = units / 0.23730
//     let start_order_cost = start_order_value * fee_rate;
//     // 0.12 = start_order_value * 0.0006

//     let close_order_value = (stop_loss_roi / 100.00) * start_order_value;
//     let close_order_cost = close_order_value * fee_rate;
//     let close_price = units / close_order_value;

//     // let revenue = units * close_transaction_price;
//     // let close_transaction_cost = units * close_transaction_price * (fee_rate * close_transaction_price);
//     // let total_cost = close_transaction_cost + open_transaction_cost;
//     // expected_balance = revenue - close_transaction_cost - open_transaction_cost
//     // expected_balance = (units * close_transaction_price) - (units * close_transaction_price * fee_rate) - open_transaction_cost;
//     // expected_balance + open_transaction_cost <=> (units * close_transaction_price) - (units * close_transaction_price * (fee_rate * close_transaction_price));
//     // (units * close_transaction_price) - (units * close_transaction_price * (fee_rate  * close_transaction_price)) = expected_balance + open_transaction_cost;
//     // (units * close_transaction_price) - (units * close_transaction_price * close_transaction_price * fee_rate) = expected_balance + open_transaction_cost;
//     // units * (close_transaction_price - (close_transaction_price^2 * fee_rate)) = expected_balance + open_transaction_cost;
//     // close_transaction_price - (close_transaction_price^2 * fee_rate) = (expected_balance + open_transaction_cost) / units;
//     // close_transaction_price * ((units) - (units * fee_rate)) = expected_balance + open_transaction_cost;
//     // close_transaction_price  = (expected_balance + open_transaction_cost) / ((units) - (units * fee_rate));
//     // close_transaction_price  = (expected_balance + open_transaction_cost) / (units * (1) - units * (fee_rate));
//     // (u * fee * cp)^2 + u * fee * cp - (eb + op) = 0
//     // (a * cp)^2 + a * cp + c = 0
//     // cp = (-b ± √(b^2 - 4ac)) / 2a
//     // a = u * fee
//     // b = u * fee
//     // c = - eb + op
//     // - u * fee ± √ (u * fee)^2 - 4 * (u * fee) * - eb + op

//     // let close_transaction_price =
//     //     (expected_balance + open_transaction_cost) / (units * (1.0 - fee_rate));
//     // println!(
//     //     "UNITS {}, expected_balance {}, start_balance {}, rel {}",
//     //     units,
//     //     expected_balance,
//     //     start_balance,
//     //     (expected_balance / start_balance)
//     // );

//     close_price
// }

// fn check_results(
//     units: f64,
//     open_cost: f64,
//     open_balance: f64,
//     limit_price: f64,
//     current_price: f64, // if position is long, check for min_price, else, if position is short, check for max_price
//     stop_loss_roi: f64,
//     leverage_factor: f64,
//     position: i32,
//     fee_rate: f64,
// ) -> Option<PositionResult> {
//     let PositionResult {
//         transaction_cost,
//         units: _units,
//         balance: _balance,
//         returns: _returns,
//         position,
//     } = close_position_taking_price(
//         position,
//         units,
//         open_cost,
//         open_balance,
//         current_price,
//         fee_rate,
//         PositionLockModifier::Nil,
//     );
//     // check if balance < 0
//     if _balance <= 0.0 {
//         return Some(PositionResult {
//             transaction_cost,
//             units: 0.0,
//             balance: 0.0,
//             returns: _returns,
//             position: 0,
//         });
//     }

//     if stop_loss_roi != 0.0 && _returns <= stop_loss_roi {}
// }

// fn close_position_taking_price(
//     current_position: i32,
//     units: f64,
//     open_cost: f64,
//     open_balance: f64,
//     current_price: f64,
//     fee_rate: f64,
//     lock: PositionLockModifier,
// ) -> PositionResult {
//     if current_position == 0 {
//         panic!("WARNING: trying to close a neutral position");
//     }
//     let fee_mg = fee_rate * current_price;
//     let transaction_price = if current_position == 1 {
//         // then, go short to close position
//         current_price - fee_mg
//     } else if current_position == -1 {
//         // else, go long to close position
//         current_price + fee_mg
//     } else {
//         panic!("Invalid position for close_position_taking_price");
//     };
//     let revenue = units * transaction_price;
//     let transaction_cost = units * transaction_price * fee_rate;
//     let total_cost = transaction_cost + open_cost;

//     let balance = revenue - total_cost;
//     let returns = (balance - open_balance) / balance;
//     println!(
//         "@@@ close_mg_position_taking_price | CURRENT POS {}, open fee {}, close fee {} total fee mg {}, result {}",
//         current_position, open_cost, transaction_cost, (open_cost + transaction_cost), returns
//     );
//     // match lock
//     match lock {
//         PositionLockModifier::Nil => PositionResult {
//             transaction_cost,
//             units: 0.0,
//             balance,
//             returns: 0.0,
//             position: 0,
//         },
//         PositionLockModifier::Fee => {
//             if total_cost >= revenue.abs() {
//                 // keeps position
//                 PositionResult {
//                     transaction_cost: 0.0,
//                     units,
//                     balance: 0.0,
//                     returns: 0.0,
//                     position: current_position,
//                 }
//             } else {
//                 PositionResult {
//                     transaction_cost,
//                     units: 0.0,
//                     balance,
//                     returns,
//                     position: 0,
//                 }
//             }
//         }
//         PositionLockModifier::Loss => {
//             if (returns - 1.0) <= 0.0 {
//                 PositionResult {
//                     transaction_cost: 0.0,
//                     units,
//                     balance: 0.0,
//                     returns: 0.0,
//                     position: current_position,
//                 }
//             } else {
//                 PositionResult {
//                     transaction_cost,
//                     units: 0.0,
//                     balance,
//                     returns,
//                     position: 0,
//                 }
//             }
//         }
//     }
// }

// #[derive(Clone)]
// pub enum StrategyReceiver {
//     MarketDataFeed(Receiver<MarketDataFeedDTE>),
// }

// impl Strategy {
//     // pub async fn listen_events(mut self) -> Result<Result<(), Error>, JoinError> {
//     //     // let observed_cloned = self.data_feed_arc.clone();

//     //     // let handle = tokio::spawn(async move {
//     //     //     let observed = observed_cloned;
//     //     //     let result = wait_for_update(&*observed).await;
//     //     //     let dto = result.0.clone();

//     //     //     self.set_benchmark(dto.lf, &dto.last_bar, &dto.traded_symbol)
//     //     // });
//     //     // handle.await
//     //     Ok(())
//     // }
// }

// impl Default for Strategy<'_> {
//     fn default() -> Self {
//         Strategy {
//             name: "Default Strategy".to_string(),
//             pre_indicator_cols: None,
//             indicators: vec![],
//             signals: vec![],
//             data_feed_arc: Arc::new(MarketDataFeed::default()),
//             // performance: Performance::default(),
//             data: LazyFrame::default(),
//         }
//     }
// }

// #[async_trait]
// impl Subscriber<StrategyReceiver> for Strategy {
//     async fn subscribe(&mut self) -> Result<(), SendError<StrategyReceiver>> {
//         for receiver in self.receivers.clone() {
//             match receiver {
//                 StrategyReceiver::MarketDataFeed(mut rx) => match rx.borrow_mut().changed().await {
//                     Ok(()) => {
//                         let dte = rx.borrow().clone();
//                         match dte {
//                             MarketDataFeedDTE::SetBenchmark(lf, last_bar, traded_symbol) => {
//                                 let _ = self.set_benchmark(lf, &last_bar, &traded_symbol);
//                             }
//                             _ => {}
//                         }
//                     }
//                     Err(err) => println!("STRATEGY SUBSCRIBE ERROR {:?}", err),
//                 },
//             }
//         }

//         Ok(())
//     }
// }

// pub enum StrategyDTE {
//     Nil,
//     SetBenchmark(LazyFrame, NaiveDateTime, String),
// }

// impl Default for StrategyDTE {
//     fn default() -> Self {
//         StrategyDTE::Nil
//     }
// }

// fn map_cols_to_clauses(cols: &Vec<String>) -> Vec<ColClause> {
//     let mut clauses: Vec<ColClause> = vec![];
//     if cols.contains(&String::from("short")) {
//         let col_clause = ColClause::new(
//             col("short").and(col("position").shift(1).eq(0)),
//             lit(-1),
//             "short".to_string(),
//         );
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("long")) {
//         let col_clause = ColClause::new(
//             col("long").and(col("position").shift(1).eq(0)),
//             lit(1),
//             "long".to_string(),
//         );
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("short_close")) {
//         let condition = col("short_close").and(col("position").shift(1).eq(-1));
//         let col_clause = ColClause::new(condition, lit(0), "short_close".to_string());
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("long_close")) {
//         let condition = col("long_close").and(col("position").shift(1).eq(1));
//         let col_clause = ColClause::new(condition, lit(0), "long_close".to_string());
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("short_reverse")) {
//         let condition = col("short_reverse").and(col("position").shift(1).eq(-1));
//         let col_clause = ColClause::new(condition, lit(1), "short_reverse".to_string());
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("position_close")) {
//         let condition = col("position_close").and(
//             col("position")
//                 .shift(1)
//                 .eq(1)
//                 .or(col("position").shift(1).eq(-1)),
//         );
//         let col_clause = ColClause::new(condition, lit(0), "position_close".to_string());
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("position_revert")) {
//         let condition = col("position_close").and(
//             col("position")
//                 .shift(1)
//                 .eq(1)
//                 .or(col("position").shift(1).eq(-1)),
//         );
//         let col_clause = ColClause::new(
//             condition,
//             col("position").shift(1) * lit(-1),
//             "position_revert".to_string(),
//         );
//         clauses.push(col_clause);
//     }
//     if cols.contains(&String::from("position_keep")) {
//         let condition = col("position_keep");
//         let col_clause = ColClause::new(
//             condition,
//             col("position").shift(1),
//             "position_keep".to_string(),
//         );
//         clauses.push(col_clause);
//     }
//     clauses
// }

// struct ColClause {
//     pub condition: Expr,
//     pub value: Expr,
//     pub signal: Expr,
// }

// impl ColClause {
//     pub fn new(condition: Expr, value: Expr, signal: String) -> Self {
//         Self {
//             condition,
//             value,
//             signal: lit(signal),
//         }
//     }
// }
