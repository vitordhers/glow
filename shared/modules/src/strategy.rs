// use super::performance::Performance;
// use chrono::{Duration as ChronoDuration, NaiveDateTime};
// use common::{
//     constants::DAY_IN_MS,
//     enums::{
//         balance::Balance, modifiers::price_level::PriceLevel, order_status::OrderStatus,
//         side::Side, signal_category::SignalCategory,
//     },
//     functions::get_symbol_ohlc_cols,
//     structs::{BehaviorSubject, Trade},
//     traits::{
//         exchange::{BenchmarkExchange, TraderHelper},
//         indicator::Indicator,
//         signal::Signal,
//     },
// };
// use exchanges::enums::TraderExchangeWrapper;
// use glow_error::GlowError;
// use indicators::IndicatorWrapper;
// use polars::prelude::*;
// use signals::SignalWrapper;
// use std::{
//     collections::{HashMap, HashSet},
//     sync::{Arc, Mutex},
//     time::Instant,
// };

// pub struct Strategy {
//     pub name: String,
//     pub pre_indicators: Vec<IndicatorWrapper>,
//     pub indicators: Vec<IndicatorWrapper>,
//     pub signals: Vec<SignalWrapper>,
//     pub benchmark_balance: f64,
//     performance: Arc<Mutex<Performance>>,
//     trader_exchange: Arc<Mutex<TraderExchangeWrapper>>,
//     pub signal_generator: BehaviorSubject<Option<SignalCategory>>,
//     pub current_balance_listener: BehaviorSubject<Balance>,
// }

// impl Strategy {
//     pub fn new(
//         name: String,
//         pre_indicators: Vec<IndicatorWrapper>,
//         indicators: Vec<IndicatorWrapper>,
//         signals: Vec<SignalWrapper>,
//         benchmark_balance: f64,
//         performance: &Arc<Mutex<Performance>>,
//         trader_exchange: &Arc<Mutex<TraderExchangeWrapper>>,
//         signal_generator: BehaviorSubject<Option<SignalCategory>>,
//         current_balance_listener: BehaviorSubject<Balance>,
//     ) -> Strategy {
//         Strategy {
//             name,
//             pre_indicators,
//             indicators,
//             signals,
//             signal_generator: signal_generator.clone(),
//             benchmark_balance,
//             performance: performance.clone(),
//             trader_exchange: trader_exchange.clone(),
//             current_balance_listener: current_balance_listener.clone(),
//         }
//     }

//     pub fn set_benchmark(
//         &self,
//         initial_tick_data_lf: LazyFrame,
//         initial_last_bar: NaiveDateTime,
//     ) -> Result<LazyFrame, GlowError> {
//         let tick_data = initial_tick_data_lf.cache();
//         let mut initial_trading_data_lf = self.set_strategy_data(tick_data)?;

//         // filter only last day positions
//         // initial last bar - 1 day
//         let last_day_datetime = initial_last_bar - ChronoDuration::milliseconds(DAY_IN_MS);
//         let benchmark_filtered_data = initial_trading_data_lf
//             .clone()
//             .filter(col("start_time").gt_eq(lit(last_day_datetime.timestamp_millis())));

//         let benchmark_trading_data = self.compute_benchmark_positions(&benchmark_filtered_data)?;

//         initial_trading_data_lf =
//             initial_trading_data_lf.left_join(benchmark_trading_data, "start_time", "start_time");

//         Ok(initial_trading_data_lf)
//     }

//     pub fn set_strategy_data(&self, tick_data: LazyFrame) -> Result<LazyFrame, GlowError> {
//         let mut lf = self.set_pre_indicators_data(&tick_data)?;
//         lf = lf.cache();
//         lf = self.set_indicators_data(&lf)?;
//         lf = lf.cache();
//         lf = self.set_signals_data(&lf)?;
//         Ok(lf)
//     }

//     fn set_pre_indicators_data(&self, data: &LazyFrame) -> Result<LazyFrame, GlowError> {
//         let mut data = data.to_owned();

//         for pre_indicator in &self.pre_indicators {
//             let lf = pre_indicator.set_indicator_columns(data.clone())?;
//             data = data.left_join(lf, "start_time", "start_time");
//         }

//         Ok(data)
//     }

//     fn set_indicators_data(&self, data: &LazyFrame) -> Result<LazyFrame, GlowError> {
//         let mut data = data.to_owned();

//         for indicator in &self.indicators {
//             let lf = indicator.set_indicator_columns(data.clone())?;
//             data = data.left_join(lf, "start_time", "start_time");
//         }

//         Ok(data)
//     }

//     fn set_signals_data(&self, data: &LazyFrame) -> Result<LazyFrame, GlowError> {
//         let mut data = data.to_owned();
//         for signal in &self.signals {
//             let lf = signal.set_signal_column(&data)?;
//             data = data.left_join(lf, "start_time", "start_time");
//         }

//         Ok(data)
//     }

//     fn compute_benchmark_positions(&self, data: &LazyFrame) -> Result<LazyFrame, GlowError> {
//         let data = data.to_owned();
//         // TODO: TRY TO IMPLEMENT THIS USING LAZYFRAMES
//         let mut df = data.clone().collect()?;
//         // let path = "data/test".to_string();
//         // let file_name = "benchmark_data.csv".to_string();
//         // save_csv(path.clone(), file_name, &df, true)?;
//         // println!("@@@@@ compute_benchmark_positions {:?}", df);

//         // uses hashset to ensure no SignalCategory is double counted
//         let mut signals_cols = HashSet::new();
//         for signal in self.signals.clone().into_iter() {
//             signals_cols.insert(String::from(signal.signal_category().get_column()));
//         }
//         let contains_short = signals_cols.contains(SignalCategory::GoShort.get_column());
//         let contains_long = signals_cols.contains(SignalCategory::GoLong.get_column());
//         let contains_short_close = signals_cols.contains(SignalCategory::CloseShort.get_column());
//         let contains_long_close = signals_cols.contains(SignalCategory::CloseLong.get_column());
//         let contains_position_close =
//             signals_cols.contains(SignalCategory::ClosePosition.get_column());

//         let contains_position_revert =
//             signals_cols.contains(SignalCategory::RevertPosition.get_column());

//         let start_timestamps_vec = df
//             .column("start_time")
//             .unwrap()
//             .datetime()
//             .unwrap()
//             .into_no_null_iter()
//             .collect::<Vec<i64>>();

//         let end_timestamps_vec = start_timestamps_vec
//             .clone()
//             .into_iter()
//             .map(|start_timestamp| start_timestamp - 1)
//             .collect::<Vec<i64>>();

//         let signals_filtered_df = df.select(&signals_cols)?;

//         let mut signals_cols_map: HashMap<String, Vec<i32>> = HashMap::new();

//         signals_cols.into_iter().for_each(|col| {
//             signals_cols_map.insert(
//                 col.clone(),
//                 signals_filtered_df
//                     .column(&col)
//                     .unwrap()
//                     .i32()
//                     .unwrap()
//                     .into_no_null_iter()
//                     .collect::<Vec<i32>>(),
//             );
//         });

//         let exchange;

//         {
//             let guard = self
//                 .trader_exchange
//                 .lock()
//                 .expect("trader exchange lock error");
//             exchange = guard.clone();
//         }

//         let traded_symbol = exchange.get_traded_symbol();

//         let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(traded_symbol);

//         let additional_cols = vec![open_col, high_col, low_col, close_col];

//         let additional_filtered_df = df.select(&additional_cols)?;
//         let mut additional_cols_map = HashMap::new();
//         additional_cols.into_iter().for_each(|col| {
//             additional_cols_map.insert(
//                 col,
//                 additional_filtered_df
//                     .column(&col)
//                     .unwrap()
//                     .f64()
//                     .unwrap()
//                     .into_no_null_iter()
//                     .collect::<Vec<f64>>(),
//             );
//         });

//         let start_time = Instant::now();

//         // fee = balance * leverage * price * fee_rate
//         // marginal fee = price * fee_rate
//         let mut trade_fees = vec![0.0];
//         let mut units = vec![0.0];
//         let mut profit_and_loss = vec![0.0];
//         let mut returns = vec![0.0];
//         let mut balances = vec![self.benchmark_balance];
//         let mut positions = vec![0];
//         let mut actions = vec![SignalCategory::KeepPosition.get_column().to_string()];

//         let trading_settings = exchange.get_trading_settings();

//         let leverage_factor = exchange.get_leverage_factor();
//         let has_leverage = leverage_factor > 1.0;

//         let price_level_modifier_map_binding = trading_settings.price_level_modifier_map.clone();

//         let stop_loss: Option<&PriceLevel> = price_level_modifier_map_binding.get("sl");
//         // let stop_loss_percentage = if stop_loss.is_some() {
//         //     let stop_loss = stop_loss.unwrap();
//         //     Some(stop_loss.get_percentage())
//         // } else {
//         //     None
//         // };

//         let take_profit = price_level_modifier_map_binding.get("tp");
//         // let take_profit_percentage = if take_profit.is_some() {
//         //     let take_profit = take_profit.unwrap();
//         //     Some(take_profit.get_percentage())
//         // } else {
//         //     None
//         // };

//         let trailing_stop_loss = price_level_modifier_map_binding.get("tsl");

//         let dataframe_height = df.height();

//         // let position_modifier = trading_settings.position_lock_modifier.clone();

//         // @@@ TODO: implement this
//         // let signals_map = get_benchmark_index_signals(&df);

//         // let mut signals_iter = signals_map.into_iter();

//         // while let Some((index, signals)) = signals_iter.next() {}

//         let open_prices_col = additional_cols_map.get(&open_col).unwrap();
//         // let high_prices_col = additional_cols_map.get(&high_col).unwrap();
//         let close_prices_col = additional_cols_map.get(&close_col).unwrap();
//         // let low_prices_col = additional_cols_map.get(&low_col).unwrap();

//         let shorts_col = if contains_short {
//             signals_cols_map
//                 .get(SignalCategory::GoShort.get_column())
//                 .unwrap()
//                 .clone()
//         } else {
//             vec![0; dataframe_height]
//         };

//         let longs_col = if contains_long {
//             signals_cols_map
//                 .get(SignalCategory::GoLong.get_column())
//                 .unwrap()
//                 .clone()
//         } else {
//             vec![0; dataframe_height]
//         };

//         // let position_closes_col = if contains_position_close {
//         //     signals_cols_map
//         //         .get(SignalCategory::ClosePosition.get_column())
//         //         .unwrap()
//         //         .clone()
//         // } else {
//         //     vec![0; dataframe_height]
//         // };

//         let short_closes_col = if contains_short_close {
//             signals_cols_map
//                 .get(SignalCategory::CloseShort.get_column())
//                 .unwrap()
//                 .clone()
//         } else {
//             vec![0; dataframe_height]
//         };

//         let long_closes_col = if contains_long_close {
//             signals_cols_map
//                 .get(SignalCategory::CloseLong.get_column())
//                 .unwrap()
//                 .clone()
//         } else {
//             vec![0; dataframe_height]
//         };

//         let mut current_trade: Option<Trade> = None;
//         let mut current_peak_returns = 0.0;
//         // let mut current_position_signal = "";

//         for index in 0..dataframe_height {
//             if index == 0 {
//                 continue;
//             }

//             // let current_order_position = current_trade
//             //     .clone()
//             //     .unwrap_or_default()
//             //     .get_current_position();

//             let current_position = positions[index - 1];
//             let current_units = units[index - 1];
//             let current_balance = balances[index - 1];

//             // println!("@@@@ {}, {}, {}", current_position, current_units, current_balance);

//             // position is neutral
//             if current_position == 0 {
//                 // and changed to short
//                 if shorts_col[index - 1] == 1 {
//                     let start_timestamp = start_timestamps_vec[index];
//                     let end_timestamp = end_timestamps_vec[index];
//                     let open_price = open_prices_col[index];
//                     let close_price = close_prices_col[index];

//                     match exchange.new_benchmark_open_order(
//                         start_timestamp,
//                         Side::Sell,
//                         current_balance,
//                         open_price,
//                     ) {
//                         Ok(open_order) => {
//                             let open_trade: Trade = open_order.clone().into();
//                             trade_fees.push(open_trade.get_executed_fees());
//                             units.push(open_order.units);
//                             let (_, trade_returns) = open_trade
//                                 .calculate_current_pnl_and_returns(end_timestamp, close_price);
//                             profit_and_loss.push(0.0);
//                             returns.push(trade_returns);
//                             let open_order_cost = open_order.get_order_cost();
//                             if open_order_cost.is_none() {
//                                 panic!("open_order_cost is none {:?}", open_order);
//                             }
//                             let open_order_cost = open_order_cost.unwrap();

//                             balances.push(f64::max(0.0, current_balance - open_order_cost));

//                             positions.push(open_order.side.into());
//                             actions.push(SignalCategory::GoShort.get_column().to_string());
//                             current_trade = Some(open_trade);
//                             // current_position_signal = shorts_col[index - 1];
//                             continue;
//                         }
//                         Err(error) => {
//                             println!("create_new_benchmark_open_order error {:?}", error);
//                         }
//                     }
//                 }
//                 // and changed to long
//                 if longs_col[index - 1] == 1 {
//                     let start_timestamp = start_timestamps_vec[index];
//                     let end_timestamp = end_timestamps_vec[index];
//                     let open_price = open_prices_col[index];
//                     let close_price = close_prices_col[index];

//                     match exchange.new_benchmark_open_order(
//                         start_timestamp,
//                         Side::Buy,
//                         current_balance,
//                         open_price,
//                     ) {
//                         Ok(open_order) => {
//                             let open_trade: Trade = open_order.clone().into();
//                             trade_fees.push(open_trade.get_executed_fees());
//                             units.push(open_order.units);
//                             let (_, trade_returns) = open_trade
//                                 .calculate_current_pnl_and_returns(end_timestamp, close_price);
//                             profit_and_loss.push(0.0);
//                             returns.push(trade_returns);
//                             let open_order_cost = open_order.get_order_cost();
//                             if open_order_cost.is_none() {
//                                 panic!("open_order_cost is none {:?}", open_order);
//                             }
//                             let open_order_cost = open_order_cost.unwrap();

//                             balances.push(f64::max(0.0, current_balance - open_order_cost));

//                             positions.push(open_order.side.into());
//                             actions.push(SignalCategory::GoLong.get_column().to_string());
//                             current_trade = Some(open_trade);
//                             // current_position_signal = longs_col[index - 1];
//                             continue;
//                         }
//                         Err(error) => {
//                             println!("create_new_benchmark_open_order error {:?}", error);
//                         }
//                     }
//                 }

//                 returns.push(0.0);
//             } else {
//                 let trade = current_trade.clone().unwrap();
//                 let current_side = trade.open_order.side;

//                 // TRANSACTION modifiers (stop loss, take profit) should be checked for closing positions regardless of signals

//                 if has_leverage
//                     || stop_loss.is_some()
//                     || take_profit.is_some()
//                     || trailing_stop_loss.is_some()
//                 {
//                     // let min_price = low_prices_col[index];
//                     // let max_price = high_prices_col[index];
//                     let prev_close_price = close_prices_col[index - 1];
//                     let prev_end_timestamp = end_timestamps_vec[index - 1];
//                     match exchange.check_price_level_modifiers(
//                         &trade,
//                         prev_end_timestamp,
//                         prev_close_price,
//                         stop_loss,
//                         take_profit,
//                         trailing_stop_loss,
//                         current_peak_returns,
//                     ) {
//                         Ok(updated_trade) => {
//                             if updated_trade.is_some() {
//                                 let closed_trade = updated_trade.unwrap();
//                                 let close_order = closed_trade.clone().close_order.unwrap();

//                                 trade_fees.push(close_order.get_executed_order_fee());
//                                 units.push(0.0);

//                                 let (pnl, trade_returns) = closed_trade.calculate_pnl_and_returns();
//                                 returns.push(trade_returns);

//                                 profit_and_loss.push(pnl);
//                                 let order_cost = closed_trade.open_order.get_order_cost().unwrap();
//                                 balances.push(current_balance + order_cost + pnl);
//                                 positions.push(0);
//                                 let action = match close_order.status {
//                                     OrderStatus::StoppedBR => SignalCategory::LeverageBankrupcty,
//                                     OrderStatus::StoppedSL => SignalCategory::StopLoss,
//                                     OrderStatus::StoppedTP => SignalCategory::TakeProfit,
//                                     OrderStatus::StoppedTSL => SignalCategory::TrailingStopLoss,
//                                     _ => SignalCategory::KeepPosition,
//                                 };

//                                 actions.push(action.get_column().to_string());
//                                 current_peak_returns = 0.0;
//                                 current_trade = None;
//                                 // current_position_signal = "";
//                                 continue;
//                             }
//                         }
//                         Err(_) => {}
//                     }
//                 }

//                 // position wasn't stopped
//                 // let was_position_closed =
//                 //     position_closes_col[index - 1] == 1 && current_side != Side::Nil;
//                 let was_long_closed = long_closes_col[index - 1] == 1 && current_side == Side::Buy;
//                 let was_short_closed =
//                     short_closes_col[index - 1] == 1 && current_side == Side::Sell;

//                 let was_position_reverted = trading_settings.signals_revert_its_opposite
//                     && (longs_col[index - 1] == 1 && current_side == Side::Sell)
//                     || (shorts_col[index - 1] == 1 && current_side == Side::Buy);

//                 if
//                 // was_position_closed ||
//                 was_long_closed || was_short_closed || was_position_reverted {
//                     let close_signal = if was_position_reverted {
//                         SignalCategory::RevertPosition
//                     } else if was_long_closed {
//                         SignalCategory::CloseLong
//                     } else if was_short_closed {
//                         SignalCategory::CloseShort
//                     }
//                     // else if was_position_closed {
//                     //     SignalCategory::ClosePosition
//                     // }
//                     else {
//                         SignalCategory::KeepPosition
//                     };

//                     let current_timestamp = start_timestamps_vec[index];
//                     let open_price = open_prices_col[index];

//                     match exchange.new_benchmark_close_order(
//                         current_timestamp,
//                         &trade.id,
//                         open_price,
//                         trade.open_order.clone(),
//                         OrderStatus::Closed,
//                     ) {
//                         Ok(close_order) => {
//                             if close_signal != SignalCategory::RevertPosition {
//                                 let updated_trade = trade.update_trade(close_order.clone())?;

//                                 trade_fees.push(close_order.get_executed_order_fee());
//                                 units.push(0.0);
//                                 let (pnl, trade_returns) =
//                                     updated_trade.calculate_pnl_and_returns();
//                                 profit_and_loss.push(pnl);
//                                 returns.push(trade_returns);
//                                 let order_cost = trade.open_order.get_order_cost().unwrap();

//                                 balances.push(current_balance + order_cost + pnl);
//                                 positions.push(0);
//                                 actions.push(close_signal.get_column().to_string());
//                                 current_trade = None;
//                                 // current_position_signal = "";
//                                 current_peak_returns = 0.0;
//                             } else {
//                                 let end_timestamp = end_timestamps_vec[index];
//                                 let close_price = close_prices_col[index];

//                                 let updated_trade = trade.update_trade(close_order.clone())?;

//                                 let mut total_fee = close_order.get_executed_order_fee();
//                                 let (pnl, trade_returns) =
//                                     updated_trade.calculate_pnl_and_returns();
//                                 profit_and_loss.push(pnl);
//                                 returns.push(trade_returns);

//                                 let order_cost = trade.open_order.get_order_cost().unwrap();
//                                 let after_close_balance = current_balance + order_cost + pnl;

//                                 match exchange.new_benchmark_open_order(
//                                     end_timestamp,
//                                     close_order.side,
//                                     after_close_balance,
//                                     close_price,
//                                 ) {
//                                     Ok(open_order) => {
//                                         let open_trade: Trade = open_order.clone().into();
//                                         total_fee += open_trade.get_executed_fees();

//                                         units.push(open_order.units);

//                                         let open_order_cost = open_order.get_order_cost();
//                                         if open_order_cost.is_none() {
//                                             panic!("open_order_cost is none {:?}", open_order);
//                                         }
//                                         let open_order_cost = open_order_cost.unwrap();

//                                         balances.push(f64::max(
//                                             0.0,
//                                             after_close_balance - open_order_cost,
//                                         ));

//                                         positions.push(open_order.side.into());
//                                         actions.push(close_signal.get_column().to_string());
//                                         current_trade = Some(open_trade);
//                                     }
//                                     Err(_) => {
//                                         units.push(0.0);
//                                         balances.push(after_close_balance);
//                                         positions.push(0);
//                                         actions.push(
//                                             SignalCategory::ClosePosition.get_column().to_string(),
//                                         );
//                                         current_trade = None;
//                                     }
//                                 }

//                                 trade_fees.push(total_fee);
//                                 current_peak_returns = 0.0;
//                             }

//                             continue;
//                         }
//                         Err(error) => {
//                             println!("create_benchmark_close_order WARNING: {:?}", error)
//                         }
//                     }
//                 }

//                 let curr_close_price = close_prices_col[index];
//                 let curr_end_timestamp = end_timestamps_vec[index];

//                 // TRANSACTION modifiers (stop loss, take profit) should be checked for closing positions regardless of signals

//                 let (_, curr_returns) =
//                     trade.calculate_current_pnl_and_returns(curr_end_timestamp, curr_close_price);

//                 if curr_returns > 0.0 && curr_returns > current_peak_returns {
//                     current_peak_returns = curr_returns;
//                 }

//                 returns.push(curr_returns);
//             }

//             trade_fees.push(0.0);
//             units.push(current_units);
//             profit_and_loss.push(0.0);
//             positions.push(current_position);
//             actions.push(SignalCategory::KeepPosition.get_column().to_string());
//             balances.push(current_balance);
//         }
//         // if last position was taken
//         if positions.last().unwrap() != &0 {
//             if let Some((before_last_order_index, _)) =
//                 positions // over positions vector
//                     .iter() // iterate over
//                     .enumerate() // an enumeration
//                     .rev() // of reversed positions
//                     .find(|(_, value)| value == &&0)
//             // until it finds where value is 0
//             {
//                 // splices results vectors to values before opening the order

//                 // note that even though the vector was reversed, before_last_order_index keeps being the original vector index. Thanks, Rust <3
//                 let range = before_last_order_index..dataframe_height;
//                 let zeroed_float_patch: Vec<f64> = range.clone().map(|_| 0.0 as f64).collect();
//                 let zeroed_integer_patch: Vec<i32> = range.clone().map(|_| 0 as i32).collect();
//                 let keep_position_action_patch: Vec<String> = range
//                     .clone()
//                     .map(|_| SignalCategory::KeepPosition.get_column().to_string())
//                     .collect();

//                 trade_fees.splice(range.clone(), zeroed_float_patch.clone());
//                 units.splice(range.clone(), zeroed_float_patch.clone());
//                 profit_and_loss.splice(range.clone(), zeroed_float_patch.clone());

//                 positions.splice(range.clone(), zeroed_integer_patch);
//                 actions.splice(range.clone(), keep_position_action_patch);

//                 let previous_balance = balances[before_last_order_index];
//                 let patch_balances: Vec<f64> =
//                     range.clone().map(|_| previous_balance as f64).collect();
//                 balances.splice(range.clone(), patch_balances);
//                 returns.splice(range.clone(), zeroed_float_patch);
//             }
//         }

//         let elapsed_time = start_time.elapsed();
//         let elapsed_millis = elapsed_time.as_nanos();
//         println!(
//             "compute_benchmark_positions => Elapsed time in nanos: {}",
//             elapsed_millis
//         );

//         let trade_fee_series = Series::new("trade_fees", trade_fees);
//         let units_series = Series::new("units", units);
//         let profit_and_loss_series = Series::new("profit_and_loss", profit_and_loss);
//         let returns_series = Series::new("returns", returns);
//         let balance_series = Series::new("balance", balances);
//         let position_series = Series::new("position", positions);
//         let action_series = Series::new("action", actions);

//         let df = df.with_column(trade_fee_series)?;
//         let df = df.with_column(units_series)?;
//         let df = df.with_column(profit_and_loss_series)?;
//         let df = df.with_column(returns_series)?;
//         let df = df.with_column(balance_series)?;
//         let df = df.with_column(position_series)?;
//         let df = df.with_column(action_series)?;

//         // let path = "data/test".to_string();
//         // let file_name = "benchmark_data.csv".to_string();
//         // save_csv(path.clone(), file_name, &df, true)?;
//         // println!("@@@@@ compute_benchmark_positions {:?}", df);

//         let result = df.clone().lazy().select([
//             col("start_time"),
//             col("trade_fees"),
//             col("units"),
//             col("profit_and_loss"),
//             col("returns"),
//             col("balance"),
//             col("position"),
//             col("action"),
//         ]);

//         Ok(result)
//     }

//     // pub fn update_positions(&self, current_trading_data: DataFrame, last_period_tick_data: DataFrame) -> Result<DataFrame, Error> {
//     //     let trading_data = current_trading_data.vstack(&last_period_tick_data)?;
//     //     let strategy_data = self.update_strategy_data(&trading_data)?;

//     //     // trading_data = self.update_trading_data(&trading_data)?;
//     //     // let path = "data/test".to_string();
//     //     // let file_name = "updated.csv".to_string();
//     //     // save_csv(path.clone(), file_name, &trading_data, true)?;

//     //     // let signal = self.generate_last_position_signal(&trading_data)?;
//     //     // self.trading_data.next(trading_data);

//     //     // self.signal_generator.next(Some(signal));

//     //     Ok(strategy_data)
//     // }

//     pub fn update_strategy_data(
//         &self,
//         current_trading_data: DataFrame,
//         last_period_tick_data: DataFrame,
//     ) -> Result<DataFrame, GlowError> {
//         // let path = "data/test".to_string();
//         // let file_name = format!("update_strategy_data_b4_{}.csv", current_timestamp_ms());
//         // save_csv(path.clone(), file_name, &current_trading_data, true)?;
//         let mut strategy_data = current_trading_data.vstack(&last_period_tick_data)?;

//         // let mut df = data.sort(["start_time"], vec![false])?;
//         strategy_data = self.update_preindicators_data(strategy_data)?;
//         strategy_data = self.update_indicators_data(strategy_data)?;
//         strategy_data = self.update_signals_data(strategy_data)?;
//         // let file_name = format!("update_strategy_data_zafter_{}.csv", current_timestamp_ms());
//         // save_csv(path.clone(), file_name, &strategy_data, true)?;
//         Ok(strategy_data)
//     }

//     fn update_preindicators_data(&self, data: DataFrame) -> Result<DataFrame, GlowError> {
//         let mut data = data.to_owned();
//         for pre_indicator in &self.pre_indicators {
//             data = pre_indicator.update_indicator_columns(&data)?;
//         }
//         Ok(data)
//     }

//     fn update_indicators_data(&self, data: DataFrame) -> Result<DataFrame, GlowError> {
//         let mut data = data.to_owned();
//         for indicator in &self.indicators {
//             data = indicator.update_indicator_columns(&data)?;
//         }
//         Ok(data)
//     }

//     fn update_signals_data(&self, data: DataFrame) -> Result<DataFrame, GlowError> {
//         let mut data = data.to_owned();
//         for signal in &self.signals {
//             data = signal.update_signal_column(&data)?;
//         }
//         Ok(data)
//     }

//     pub fn generate_last_position_signal(
//         &self,
//         data: &DataFrame,
//     ) -> Result<SignalCategory, GlowError> {
//         // uses hashset to ensure no SignalCategory is double counted
//         let mut signals_cols = HashSet::new();
//         for signal in self.signals.clone().into_iter() {
//             signals_cols.insert(String::from(signal.signal_category().get_column()));
//         }

//         let contains_short = signals_cols.contains(SignalCategory::GoShort.get_column());

//         let contains_long = signals_cols.contains(SignalCategory::GoLong.get_column());

//         let contains_short_close = signals_cols.contains(SignalCategory::CloseShort.get_column());

//         let contains_long_close = signals_cols.contains(SignalCategory::CloseLong.get_column());

//         let contains_position_close =
//             signals_cols.contains(SignalCategory::ClosePosition.get_column());

//         let signals_filtered_df = data.select(&signals_cols)?;

//         let mut signals_cols_map = HashMap::new();

//         signals_cols.clone().into_iter().for_each(|col| {
//             signals_cols_map.insert(
//                 col.clone(),
//                 signals_filtered_df
//                     .column(&col)
//                     .unwrap()
//                     .i32()
//                     .unwrap()
//                     .into_no_null_iter()
//                     .collect::<Vec<i32>>(),
//             );
//         });

//         let last_index = data.height() - 1;
//         // let penultimate_index = last_index - 1;

//         let positions = data
//             .column("position")?
//             .i32()
//             .unwrap()
//             .into_iter()
//             .collect::<Vec<_>>();
//         let current_position = positions[last_index].unwrap();
//         // let mut selection = vec!["start_time".to_string()];
//         // selection.extend(signals_cols);
//         // println!(
//         //     "generate_last_position_signal DATA TAIL {:?}",
//         //     data.select(selection)?.tail(Some(1))
//         // );

//         // position is neutral
//         if current_position == 0 {
//             // println!("{} | generate_last_position_signal -> last_index ={}, last start_time = {}, current_position = {}", current_timestamp_ms(), &last_index, last_start_time,  &current_position);
//             if contains_short
//                 && signals_cols_map
//                     .get(SignalCategory::GoShort.get_column())
//                     .unwrap()[last_index]
//                     == 1
//             {
//                 return Ok(SignalCategory::GoShort);
//             }

//             if contains_long
//                 && signals_cols_map
//                     .get(SignalCategory::GoLong.get_column())
//                     .unwrap()[last_index]
//                     == 1
//             {
//                 return Ok(SignalCategory::GoLong);
//             }
//         } else {
//             // position is not closed
//             let is_position_close = contains_position_close
//                 && signals_cols_map
//                     .get(SignalCategory::ClosePosition.get_column())
//                     .unwrap()[last_index]
//                     == 1;

//             let is_long_close = current_position == 1
//                 && contains_long_close
//                 && signals_cols_map
//                     .get(SignalCategory::CloseLong.get_column())
//                     .unwrap()[last_index]
//                     == 1;
//             // println!(
//             //     "is_long_close = {}, contains_long_close = {} signals_cols_map.get = {}",
//             //     is_long_close,
//             //     contains_long_close,
//             //     signals_cols_map
//             //         .get(SignalCategory::CloseLong.get_column())
//             //         .unwrap()[last_index]
//             // );

//             let is_short_close = current_position == -1
//                 && contains_short_close
//                 && signals_cols_map
//                     .get(SignalCategory::CloseShort.get_column())
//                     .unwrap()[last_index]
//                     == 1;

//             // println!(
//             //     "is_short_close = {}, contains_short_close = {} signals_cols_map.get = {}",
//             //     is_short_close,
//             //     contains_short_close,
//             //     signals_cols_map
//             //         .get(SignalCategory::CloseShort.get_column())
//             //         .unwrap()[last_index]
//             // );

//             // println!("{} | generate_last_position_signal -> last_index ={}, last start_time = {}, current_position = {}", current_timestamp_ms(), &last_index, last_start_time,  &current_position);

//             if is_position_close || is_long_close || is_short_close {
//                 let close_signal = if is_position_close {
//                     SignalCategory::ClosePosition
//                 } else if is_short_close {
//                     SignalCategory::CloseShort
//                 } else if is_long_close {
//                     SignalCategory::CloseLong
//                 } else {
//                     return Ok(SignalCategory::KeepPosition);
//                 };

//                 return Ok(close_signal);
//             }
//         }

//         Ok(SignalCategory::KeepPosition)
//     }
// }

// fn get_benchmark_index_signals(df: &DataFrame) -> HashMap<usize, Vec<SignalCategory>> {
//     let mut signals_map = HashMap::new();

//     let signals_categories = vec![
//         SignalCategory::GoShort,
//         SignalCategory::GoLong,
//         SignalCategory::CloseLong,
//         SignalCategory::CloseShort,
//     ];

//     let mut iter = signals_categories.into_iter();

//     while let Some(signal_category) = iter.next() {
//         match df.column(signal_category.get_column()) {
//             Ok(short_col) => {
//                 short_col
//                     .i32()
//                     .unwrap()
//                     .into_no_null_iter()
//                     .enumerate()
//                     .for_each(|(index, value)| {
//                         if value == 1 {
//                             signals_map
//                                 .entry(index)
//                                 .or_insert(Vec::new())
//                                 .push(signal_category);
//                         }
//                     });
//             }
//             _ => {}
//         }
//     }

//     signals_map
// }
