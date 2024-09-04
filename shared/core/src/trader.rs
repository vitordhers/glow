use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex}};
use common::{enums::{balance::Balance, modifiers::{leverage::Leverage, price_level::PriceLevel}, order_action::OrderAction, side::Side, signal_category::SignalCategory, trade_status::TradeStatus, trading_data_update::StrategyDataUpdate}, functions::{check_last_index_for_signal, current_datetime, current_timestamp_ms, get_symbol_close_col, get_trading_columns_values}, structs::{BehaviorSubject, Execution, Order, Trade, TradingSettings}};
use exchanges::enums::TraderExchangeWrapper;
use glow_error::GlowError;
use polars::prelude::*;
use tokio::{spawn, task::JoinHandle};
use super::{data_feed::DataFeed, performance::Performance};
use futures_util::StreamExt;
use common::traits::exchange::TraderExchange;
use common::traits::exchange::TraderHelper;

#[derive(Clone)]
pub struct Trader {
    benchmark_data_emitter: BehaviorSubject<DataFrame>,
    current_balance_listener: BehaviorSubject<Balance>,
    current_trade_listener: BehaviorSubject<Option<Trade>>,
    exchange_socket_error_arc: Arc<Mutex<Option<i64>>>,
    exchange_listener: BehaviorSubject<TraderExchangeWrapper>,
    signal_listener: BehaviorSubject<SignalCategory>,
    temp_executions_arc: Arc<Mutex<Vec<Execution>>>,
    strategy_data_listener: BehaviorSubject<StrategyDataUpdate>,
    trading_data: Arc<Mutex<DataFrame>>,
    update_balance_listener: BehaviorSubject<Option<Balance>>,
    update_order_listener: BehaviorSubject<Option<OrderAction>>,
    update_executions_listener: BehaviorSubject<Vec<Execution>>,
}

impl Trader {
    pub fn new(
        benchmark_data_emitter: &BehaviorSubject<DataFrame>,
        current_balance_listener: &BehaviorSubject<Balance>,
        current_trade_listener: &BehaviorSubject<Option<Trade>>,

        exchange_socket_error_arc: &Arc<Mutex<Option<i64>>>,
        exchange_listener: &BehaviorSubject<TraderExchangeWrapper>,
        
        signal_listener: &BehaviorSubject<SignalCategory>,
        
        strategy_data_listener: &BehaviorSubject<StrategyDataUpdate>,
        trading_data: &Arc<Mutex<DataFrame>>,
        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        
    ) -> Trader {
        Trader {
            benchmark_data_emitter: benchmark_data_emitter.clone(),
            current_balance_listener: current_balance_listener.clone(),
            current_trade_listener: current_trade_listener.clone(),

            exchange_socket_error_arc: exchange_socket_error_arc.clone(),
            exchange_listener: exchange_listener.clone(),
            
            signal_listener: signal_listener.clone(),
            temp_executions_arc: Arc::new(Mutex::new(Vec::new())),
            
            strategy_data_listener: strategy_data_listener.clone(),
            trading_data: trading_data.clone(),
            update_balance_listener: update_balance_listener.clone(),
            update_order_listener: update_order_listener.clone(),
            update_executions_listener: update_executions_listener.clone(),
            
        }
    }

    async fn process_last_signal(
        &self,
        signal: SignalCategory,

    ) -> Result<(), GlowError> {
        // todo!("");
        let current_trade = self.current_trade_listener.value();
        let exchange = self.exchange_listener.value();
        let traded_symbol = exchange.get_traded_symbol();
        let close_col = traded_symbol.get_close_col();
        let trading_data_binding = self.trading_data_listener.value();
        // TODO: check if this can be received via param
        let last_price = trading_data_binding
            .column(&close_col)?
            .f64()?
            .into_no_null_iter()
            .last()
            .expect("process_last_signal -> SignalCategory::GoLong -> missing last price");
        
        if current_trade.is_none() {
            let available_to_withdraw = self.current_balance_listener.value().available_to_withdraw;
            return Ok(open_order(
                exchange,
                signal.into(),
                available_to_withdraw,
                last_price,
            )
            .await?)
            
        }
        let mut current_trade = current_trade.unwrap();
        let current_trade_status = &current_trade.status();
        let open_order_side = current_trade.open_order.side;
        match (current_trade_status, signal, open_order_side) {
            (TradeStatus::New, SignalCategory::CloseLong, Side::Buy) | // non-executed order received close signal
            (TradeStatus::New, SignalCategory::CloseShort, Side::Sell) | // non-executed order received close signal
            (TradeStatus::New, SignalCategory::ClosePosition, _) | // non-executed order received close signal
            (TradeStatus::New, SignalCategory::GoLong, Side::Sell) | // non-executed order received opposite signal
            (TradeStatus::New, SignalCategory::GoShort, Side::Buy) // non-executed order received opposite signal
             => {
                match exchange
                        .cancel_order(current_trade.open_order.id.clone())
                        .await
                    {
                        Ok(cancel_result) => {
                            // TODO: separate in fn
                            if cancel_result {
                                if signal == SignalCategory::CloseLong || signal == SignalCategory::CloseShort || signal == SignalCategory::ClosePosition {
                                    // simple close signal received
                                    println!(
                                        "\n{:?} | ⚠️ Current order {:?} position, without executions, will be cancelled as it received a close signal.",
                                        current_datetime(), 
                                        current_trade.open_order.side
                                    );
                                    return Ok(())
                                }
                                println!(
                                    "\n{:?} | ⚠️ Current idle order {:?} position, without executions, will be cancelled as it received an opposite side open signal.",
                                    current_datetime(),
                                    current_trade.open_order.side
                                );

                                let wallet_balance = self.current_balance_listener.value().wallet_balance;

                                match open_order(
                                    exchange,
                                    signal.into(),
                                    wallet_balance,
                                    last_price,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        println!(
                                            "\n{:?} | ♻️ Current idle order, {:?} position, will be recycled as it received an opposite side open signal.",
                                            current_datetime(),
                                            current_trade.open_order.side
                                        );
                                        Ok(())
                                    }
                                    Err(error) => {
                                        let error = format!("TradeStatus::New -> Recycle Idle Position -> open new position failed! {:?}", error);
                                        let error = GlowError::new(String::from("Cancel Order Error"), error);
                                        Err(error)
                                    }
                                }
                            } else {
                                let error =
                                    GlowError::new(String::from("Trade Error"),"TradeStatus::New -> Cancel Idle Position -> cancel order returned false".to_string());
                                Err(error)
                            }
                        }
                        Err(error) => {
                            let error = format!("TradeStatus::New -> Cancel Idle Position -> cancel result failed! {:?}", error);
                            let error = GlowError::new(String::from("Trade Error"), error);
                            Err(error)
                        }
                    }
            }
            (TradeStatus::PartiallyOpen | TradeStatus::PendingCloseOrder, signal, open_order_side) => {
                if current_trade_status == &TradeStatus::PartiallyOpen {
                    let mut open_order = current_trade.open_order.clone();
                    let left_units = open_order.get_executed_quantity() - open_order.units;
                    let updated_units = Some(left_units);
                    let updated_price = None;
                    let updated_stop_loss_price = None;
                    let updated_take_profit_price = None;
                    let amend_result = exchange
                        .amend_order(
                            current_trade.open_order.id.clone(),
                            updated_units,
                            updated_price,
                            updated_stop_loss_price,
                            updated_take_profit_price,
                        )
                        .await;
                    match amend_result {
                        Ok(amended) => {
                            if amended {
                                open_order.update_units(left_units);
                                current_trade = current_trade.update_trade(open_order)?;
                            } else {
                                let error = format!(
                                    "TradeStatus::PartiallyOpen -> amend order returned false"
                                );
                                let error = GlowError::new(String::from("Amend Order Error"), error);
                                return Err(error);
                            }
                        }
                        Err(error) => {
                            let error = format!(
                                "TradeStatus::PartiallyOpen -> amend result failed! {:?}",
                                error
                            );
                            let error = GlowError::new(String::from("Amend Order Error"), error);
                            return Err(error);
                        }
                    }
                }
    
                match exchange
                    .try_close_position(
                        &current_trade,
                        last_price,
                    )
                    .await
                {
                    Ok(close_order) => {
                        println!("TradeStatus::PartiallyOpen | TradeStatus::PendingCloseOrder -> try_close_position result {:?}", close_order);
                        Ok(())},
                    Err(error) => {
                        let error = format!(
                            "TradeStatus::PartiallyOpen | TradeStatus::PendingCloseOrder -> try close position result failed! {:?}",
                            error
                        );
                        let error = GlowError::new(String::from("Try Close Position Error"), error);
                        Err(error)
                    }
                }
            }
            (current_trade_status, signal, open_order_side) => {
                println!("process_last_signal NOOP current_trade_status = {:?}, signal = {:?}, open_order_side = {:?}", current_trade_status, signal, open_order_side);
                Ok(())
            },
        }
        
    }

    fn init_signal_handler(&self
    ) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.signal_listener.subscribe();
            while let Some(signal) = subscription.next().await {
                if signal == SignalCategory::KeepPosition {
                    continue;
                }
                match trader.process_last_signal(
                    signal
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        println!("process_last_signal error {:?}", error);
                    }
                }
            }
        })
    }

    // TODO: check if it makes sense to have two behavior subjects for apparently the same data
    fn init_balance_update_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.update_balance_listener.subscribe();
            while let Some(balance_update) = subscription.next().await {
                if let Some(balance) = balance_update {
                    trader.current_balance_listener.next(balance)
                }
            }
        })
    }

    fn init_order_update_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.update_order_listener.subscribe();
            while let Some(order_update) = subscription.next().await {
                if order_update.is_none() {
                    continue;
                }
                let order_action = order_update.unwrap();
                let current_trade = trader.current_trade_listener.value();
                match order_action.clone() {
                    OrderAction::Update(mut updated_order) | OrderAction::Stop(mut updated_order) => {
                        updated_order = add_executions_to_order_and_remove_from_temp(
                            &trader.temp_executions_arc,
                            updated_order,
                        );

                        if current_trade.is_none() {
                            if updated_order.is_stop {
                                println!("OrderAction::Update | OrderAction::Stop -> received a stop order update with an empty trade");
                                continue;
                            }
                            if updated_order.is_close {
                                println!("OrderAction::Update | OrderAction::Stop -> received a close order update with an empty trade");
                                continue;
                            }
                            println!(
                                "\n{:?} | 📖 Opened {:?} order ({:?} units)",
                                current_datetime(),
                                updated_order.side,
                                &updated_order.units,
                            );
                            let new_trade = Trade::new(
                                updated_order,
                                None,
                            );
                            trader.current_trade_listener.next(Some(new_trade));
                            continue;
                        }
                        let current_trade = current_trade.unwrap();
                        match current_trade.update_trade(updated_order.clone()) {
                            Ok(updated_trade) => {
                                // println!("match trade, updated {:?}", &updated_trade);
                                if let OrderAction::Stop(_) = order_action {
                                    let (pnl, returns) =
                                        updated_trade.calculate_pnl_and_returns();
                                    println!("\n{:?} | {} Position {:?} was stopped. Profit and loss = {}, returns = {}",  current_datetime(),
                                    if pnl > 0.0 { "📈" } else { "📉" },
                                    updated_trade.open_order.side,
                                    pnl,
                                    returns);
                                }
                                trader.current_trade_listener.next(Some(updated_trade));
                            }
                            Err(error) => {
                                println!(
                                    "OrderAction::Update | OrderAction::Stop -> error while updating -> error {:?}, trade = {:?}, order = {:?}",
                                    error,
                                    current_trade,
                                    updated_order,
                                );
                            }
                        }
                    }
                    OrderAction::Cancel(cancelled_order) => {
                        if current_trade.is_none() {
                            println!(
                                "OrderAction::Cancel -> cancelled order not related to current open order.\nCancelled order = {:?}\nCurrent_trade = {:?}\n",
                                cancelled_order, current_trade
                            );
                            continue;
                        }
                        let current_trade = current_trade.unwrap();
                        // check if cancelled order is open order
                        if cancelled_order.id != current_trade.open_order.id {
                            println!(
                                "OrderAction::Cancel -> cancelled order not related to current open order.\nCancelled order = {:?}\nCurrent_trade = {:?}\n",
                                cancelled_order, current_trade
                            );
                            continue;
                        }
                        let cancelled_order = current_trade.open_order.cancel();
                        let updated_trade = current_trade
                            .update_trade(cancelled_order)
                            .expect("OrderAction::Cancel -> update_trade unwrap");

                        trader.current_trade_listener.next(Some(updated_trade));
                    }
                }
            }
        })
    }

    fn init_executions_update_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.update_executions_listener.subscribe();
            while let Some(latest_executions) = subscription.next().await {
                if latest_executions.len() == 0 {
                    continue;
                }
    
                let mut temp_executions_guard = trader.temp_executions_arc
                    .lock()
                    .expect("get_actions_handle -> temp_executions_guard deadlock");
                temp_executions_guard.extend(latest_executions);
                println!(
                    "temp_executions_guard lenght {}",
                    temp_executions_guard.len()
                );
            }
        })
    }

    // TODO: refactor this
    fn on_close_update_trading_data(
        &self,
        strategy_updated_data: DataFrame,        
    ) -> Result<Option<DataFrame>, GlowError> {
        println!("{} on_close_update_trading_data", current_timestamp_ms());
        // missing trade_fees, units, profit_and_loss, returns, balance, position, action
        let mut strategy_updated_data_clone = strategy_updated_data.clone();
        
        let (start_times,
            mut trades_fees,
            mut units,
            mut pnl,
            mut returns,
            mut balances,
            mut positions,
            mut actions) = get_trading_columns_values(&strategy_updated_data)?;
    
        let index = start_times.len() - 1;
    
        let balance = self.current_balance_listener.value();
        balances[index] = Some(balance.available_to_withdraw);
        let signal = self.signal_listener.value().unwrap_or_default();
        actions[index] = Some(signal.get_column());
        let trade = self.current_trade_listener.value();
    
        let mut result = None;
        match trade {
            Some(current_trade) => {
                let trade_status = current_trade.status();
                match trade_status {
                    TradeStatus::Cancelled | TradeStatus::Closed => {
                        if trade_status == TradeStatus::Cancelled {
                            trades_fees[index] = Some(0.0);
                            units[index] = Some(0.0);
                            pnl[index] = Some(0.0);
                            returns[index] = Some(0.0);
                            positions[index] = Some(0);
                        } else {
                            let interval_start_timestamp =  start_times[index]
                                    .expect("update_trading_data -> TradeStatus::Closed arm -> interval_start_timestamp unwrap");
                            let interval_end_timestamp = current_timestamp_ms() as i64;
    
                            let (profit_and_loss, current_returns) =
                                current_trade.calculate_pnl_and_returns();
    
                            let interval_fee = current_trade.get_executed_fees_between_interval(
                                interval_start_timestamp,
                                interval_end_timestamp,
                            );
    
                            trades_fees[index] = Some(interval_fee);
                            units[index] = Some(0.0);
                            pnl[index] = Some(profit_and_loss);
                            returns[index] = Some(current_returns);
                            positions[index] = Some(0);
                        }
    
                        // updates df
                        strategy_updated_data_clone
                            .replace("trade_fees", Series::new("trade_fees", trades_fees))?;
                        strategy_updated_data_clone
                            .replace("units", Series::new("units", units))?;
                        strategy_updated_data_clone
                            .replace("profit_and_loss", Series::new("profit_and_loss", pnl))?;
                        strategy_updated_data_clone
                            .replace("returns", Series::new("returns", returns))?;
                        strategy_updated_data_clone
                            .replace("balance", Series::new("balance", balances))?;
                        strategy_updated_data_clone
                            .replace("position", Series::new("position", positions))?;
                        strategy_updated_data_clone
                            .replace("action", Series::new("action", actions))?;
                        result = Some(strategy_updated_data_clone);
                    }
                    _ => {}
                }
            }
            None => {}
        }
    
        Ok(result)
    }

    // TODO: refactor this
    fn init_trade_update_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.current_trade_listener.subscribe();
            while let Some(current_trade) = subscription.next().await {
                if current_trade.is_none() {
                    continue;
                }
    
                let current_trade = current_trade.unwrap();
                let trade_status = current_trade.status();
                if trade_status == TradeStatus::Cancelled || trade_status == TradeStatus::Closed {
                    if trade_status == TradeStatus::Closed {
                        let close_order = current_trade.clone().close_order.unwrap();
                        let (pnl, returns) = current_trade.calculate_pnl_and_returns();
                        println!(
                            "\n{:?} | 📕 Closed Order {:?} side ({:?} units), profit/loss: {}, returns: {}",
                            current_datetime(),
                            current_trade.open_order.side,
                            &close_order.units,
                            pnl,
                            returns
                        );
                    } else {
                        println!(
                            "\n{:?} | ❌ Current Order side {:?} cancelled successfully!",
                            current_datetime(),
                            current_trade.open_order.side,
                        );
                    }
    
                    // let trading_data = trader.trading_data_listener.value();
                    // let updated_trading_data = trader.on_close_update_trading_data(
                    //     trading_data
                    // )
                    // .expect(
                    //     "get_current_trade_update_handle -> on_close_update_trading_data unwrap failed",
                    // );
                    // if updated_trading_data.is_some() {
                    //     let updated_trading_data = updated_trading_data.unwrap();
                    //     trader.trading_data_listener.next(updated_trading_data);
                    // }
    
                    trader.current_trade_listener.next(None);
                }
            }
        })
    }    

    
    fn compute_benchmark_positions(&self, initial_strategy_df: DataFrame) -> Result<DataFrame, GlowError> {
        todo!()
        // let data = data.to_owned();
        // // TODO: TRY TO IMPLEMENT THIS USING LAZYFRAMES
        // let mut df = data.clone().collect()?;
        // // let path = "data/test".to_string();
        // // let file_name = "benchmark_data.csv".to_string();
        // // save_csv(path.clone(), file_name, &df, true)?;
        // // println!("@@@@@ compute_benchmark_positions {:?}", df);
    
        // // uses hashset to ensure no SignalCategory is double counted
        // let mut signals_cols = HashSet::new();
        // for signal in self.signals.clone().into_iter() {
        //     signals_cols.insert(String::from(signal.signal_category().get_column()));
        // }
        // let contains_short = signals_cols.contains(SignalCategory::GoShort.get_column());
        // let contains_long = signals_cols.contains(SignalCategory::GoLong.get_column());
        // let contains_short_close = signals_cols.contains(SignalCategory::CloseShort.get_column());
        // let contains_long_close = signals_cols.contains(SignalCategory::CloseLong.get_column());
        // let contains_position_close =
        //     signals_cols.contains(SignalCategory::ClosePosition.get_column());
    
        // let contains_position_revert =
        //     signals_cols.contains(SignalCategory::RevertPosition.get_column());
    
        // let start_timestamps_vec = df
        //     .column("start_time")
        //     .unwrap()
        //     .datetime()
        //     .unwrap()
        //     .into_no_null_iter()
        //     .collect::<Vec<i64>>();
    
        // let end_timestamps_vec = start_timestamps_vec
        //     .clone()
        //     .into_iter()
        //     .map(|start_timestamp| start_timestamp - 1)
        //     .collect::<Vec<i64>>();
    
        // let signals_filtered_df = df.select(&signals_cols)?;
    
        // let mut signals_cols_map: HashMap<String, Vec<i32>> = HashMap::new();
    
        // signals_cols.into_iter().for_each(|col| {
        //     signals_cols_map.insert(
        //         col.clone(),
        //         signals_filtered_df
        //             .column(&col)
        //             .unwrap()
        //             .i32()
        //             .unwrap()
        //             .into_no_null_iter()
        //             .collect::<Vec<i32>>(),
        //     );
        // });
    
        // let exchange;
    
        // {
        //     let guard = self
        //         .trader_exchange
        //         .lock()
        //         .expect("trader exchange lock error");
        //     exchange = guard.clone();
        // }
    
        // let traded_symbol = exchange.get_traded_symbol();
    
        // let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(traded_symbol);
    
        // let additional_cols = vec![open_col, high_col, low_col, close_col];
    
        // let additional_filtered_df = df.select(&additional_cols)?;
        // let mut additional_cols_map = HashMap::new();
        // additional_cols.into_iter().for_each(|col| {
        //     additional_cols_map.insert(
        //         col,
        //         additional_filtered_df
        //             .column(&col)
        //             .unwrap()
        //             .f64()
        //             .unwrap()
        //             .into_no_null_iter()
        //             .collect::<Vec<f64>>(),
        //     );
        // });
    
        // let start_time = Instant::now();
    
        // // fee = balance * leverage * price * fee_rate
        // // marginal fee = price * fee_rate
        // let mut trade_fees = vec![0.0];
        // let mut units = vec![0.0];
        // let mut profit_and_loss = vec![0.0];
        // let mut returns = vec![0.0];
        // let mut balances = vec![self.benchmark_balance];
        // let mut positions = vec![0];
        // let mut actions = vec![SignalCategory::KeepPosition.get_column().to_string()];
    
        // let trading_settings = exchange.get_trading_settings();
    
        // let leverage_factor = exchange.get_leverage_factor();
        // let has_leverage = leverage_factor > 1.0;
    
        // let price_level_modifier_map_binding = trading_settings.price_level_modifier_map.clone();
    
        // let stop_loss: Option<&PriceLevel> = price_level_modifier_map_binding.get("sl");
        // // let stop_loss_percentage = if stop_loss.is_some() {
        // //     let stop_loss = stop_loss.unwrap();
        // //     Some(stop_loss.get_percentage())
        // // } else {
        // //     None
        // // };
    
        // let take_profit = price_level_modifier_map_binding.get("tp");
        // // let take_profit_percentage = if take_profit.is_some() {
        // //     let take_profit = take_profit.unwrap();
        // //     Some(take_profit.get_percentage())
        // // } else {
        // //     None
        // // };
    
        // let trailing_stop_loss = price_level_modifier_map_binding.get("tsl");
    
        // let dataframe_height = df.height();
    
        // // let position_modifier = trading_settings.position_lock_modifier.clone();
    
        // // @@@ TODO: implement this
        // // let signals_map = get_benchmark_index_signals(&df);
    
        // // let mut signals_iter = signals_map.into_iter();
    
        // // while let Some((index, signals)) = signals_iter.next() {}
    
        // let open_prices_col = additional_cols_map.get(&open_col).unwrap();
        // // let high_prices_col = additional_cols_map.get(&high_col).unwrap();
        // let close_prices_col = additional_cols_map.get(&close_col).unwrap();
        // // let low_prices_col = additional_cols_map.get(&low_col).unwrap();
    
        // let shorts_col = if contains_short {
        //     signals_cols_map
        //         .get(SignalCategory::GoShort.get_column())
        //         .unwrap()
        //         .clone()
        // } else {
        //     vec![0; dataframe_height]
        // };
    
        // let longs_col = if contains_long {
        //     signals_cols_map
        //         .get(SignalCategory::GoLong.get_column())
        //         .unwrap()
        //         .clone()
        // } else {
        //     vec![0; dataframe_height]
        // };
    
        // // let position_closes_col = if contains_position_close {
        // //     signals_cols_map
        // //         .get(SignalCategory::ClosePosition.get_column())
        // //         .unwrap()
        // //         .clone()
        // // } else {
        // //     vec![0; dataframe_height]
        // // };
    
        // let short_closes_col = if contains_short_close {
        //     signals_cols_map
        //         .get(SignalCategory::CloseShort.get_column())
        //         .unwrap()
        //         .clone()
        // } else {
        //     vec![0; dataframe_height]
        // };
    
        // let long_closes_col = if contains_long_close {
        //     signals_cols_map
        //         .get(SignalCategory::CloseLong.get_column())
        //         .unwrap()
        //         .clone()
        // } else {
        //     vec![0; dataframe_height]
        // };
    
        // let mut current_trade: Option<Trade> = None;
        // let mut current_peak_returns = 0.0;
        // // let mut current_position_signal = "";
    
        // for index in 0..dataframe_height {
        //     if index == 0 {
        //         continue;
        //     }
    
        //     // let current_order_position = current_trade
        //     //     .clone()
        //     //     .unwrap_or_default()
        //     //     .get_current_position();
    
        //     let current_position = positions[index - 1];
        //     let current_units = units[index - 1];
        //     let current_balance = balances[index - 1];
    
        //     // println!("@@@@ {}, {}, {}", current_position, current_units, current_balance);
    
        //     // position is neutral
        //     if current_position == 0 {
        //         // and changed to short
        //         if shorts_col[index - 1] == 1 {
        //             let start_timestamp = start_timestamps_vec[index];
        //             let end_timestamp = end_timestamps_vec[index];
        //             let open_price = open_prices_col[index];
        //             let close_price = close_prices_col[index];
    
        //             match exchange.new_benchmark_open_order(
        //                 start_timestamp,
        //                 Side::Sell,
        //                 current_balance,
        //                 open_price,
        //             ) {
        //                 Ok(open_order) => {
        //                     let open_trade: Trade = open_order.clone().into();
        //                     trade_fees.push(open_trade.get_executed_fees());
        //                     units.push(open_order.units);
        //                     let (_, trade_returns) = open_trade
        //                         .calculate_current_pnl_and_returns(end_timestamp, close_price);
        //                     profit_and_loss.push(0.0);
        //                     returns.push(trade_returns);
        //                     let open_order_cost = open_order.get_order_cost();
        //                     if open_order_cost.is_none() {
        //                         panic!("open_order_cost is none {:?}", open_order);
        //                     }
        //                     let open_order_cost = open_order_cost.unwrap();
    
        //                     balances.push(f64::max(0.0, current_balance - open_order_cost));
    
        //                     positions.push(open_order.side.into());
        //                     actions.push(SignalCategory::GoShort.get_column().to_string());
        //                     current_trade = Some(open_trade);
        //                     // current_position_signal = shorts_col[index - 1];
        //                     continue;
        //                 }
        //                 Err(error) => {
        //                     println!("create_new_benchmark_open_order error {:?}", error);
        //                 }
        //             }
        //         }
        //         // and changed to long
        //         if longs_col[index - 1] == 1 {
        //             let start_timestamp = start_timestamps_vec[index];
        //             let end_timestamp = end_timestamps_vec[index];
        //             let open_price = open_prices_col[index];
        //             let close_price = close_prices_col[index];
    
        //             match exchange.new_benchmark_open_order(
        //                 start_timestamp,
        //                 Side::Buy,
        //                 current_balance,
        //                 open_price,
        //             ) {
        //                 Ok(open_order) => {
        //                     let open_trade: Trade = open_order.clone().into();
        //                     trade_fees.push(open_trade.get_executed_fees());
        //                     units.push(open_order.units);
        //                     let (_, trade_returns) = open_trade
        //                         .calculate_current_pnl_and_returns(end_timestamp, close_price);
        //                     profit_and_loss.push(0.0);
        //                     returns.push(trade_returns);
        //                     let open_order_cost = open_order.get_order_cost();
        //                     if open_order_cost.is_none() {
        //                         panic!("open_order_cost is none {:?}", open_order);
        //                     }
        //                     let open_order_cost = open_order_cost.unwrap();
    
        //                     balances.push(f64::max(0.0, current_balance - open_order_cost));
    
        //                     positions.push(open_order.side.into());
        //                     actions.push(SignalCategory::GoLong.get_column().to_string());
        //                     current_trade = Some(open_trade);
        //                     // current_position_signal = longs_col[index - 1];
        //                     continue;
        //                 }
        //                 Err(error) => {
        //                     println!("create_new_benchmark_open_order error {:?}", error);
        //                 }
        //             }
        //         }
    
        //         returns.push(0.0);
        //     } else {
        //         let trade = current_trade.clone().unwrap();
        //         let current_side = trade.open_order.side;
    
        //         // TRANSACTION modifiers (stop loss, take profit) should be checked for closing positions regardless of signals
    
        //         if has_leverage
        //             || stop_loss.is_some()
        //             || take_profit.is_some()
        //             || trailing_stop_loss.is_some()
        //         {
        //             // let min_price = low_prices_col[index];
        //             // let max_price = high_prices_col[index];
        //             let prev_close_price = close_prices_col[index - 1];
        //             let prev_end_timestamp = end_timestamps_vec[index - 1];
        //             match exchange.check_price_level_modifiers(
        //                 &trade,
        //                 prev_end_timestamp,
        //                 prev_close_price,
        //                 stop_loss,
        //                 take_profit,
        //                 trailing_stop_loss,
        //                 current_peak_returns,
        //             ) {
        //                 Ok(updated_trade) => {
        //                     if updated_trade.is_some() {
        //                         let closed_trade = updated_trade.unwrap();
        //                         let close_order = closed_trade.clone().close_order.unwrap();
    
        //                         trade_fees.push(close_order.get_executed_order_fee());
        //                         units.push(0.0);
    
        //                         let (pnl, trade_returns) = closed_trade.calculate_pnl_and_returns();
        //                         returns.push(trade_returns);
    
        //                         profit_and_loss.push(pnl);
        //                         let order_cost = closed_trade.open_order.get_order_cost().unwrap();
        //                         balances.push(current_balance + order_cost + pnl);
        //                         positions.push(0);
        //                         let action = match close_order.status {
        //                             OrderStatus::StoppedBR => SignalCategory::LeverageBankrupcty,
        //                             OrderStatus::StoppedSL => SignalCategory::StopLoss,
        //                             OrderStatus::StoppedTP => SignalCategory::TakeProfit,
        //                             OrderStatus::StoppedTSL => SignalCategory::TrailingStopLoss,
        //                             _ => SignalCategory::KeepPosition,
        //                         };
    
        //                         actions.push(action.get_column().to_string());
        //                         current_peak_returns = 0.0;
        //                         current_trade = None;
        //                         // current_position_signal = "";
        //                         continue;
        //                     }
        //                 }
        //                 Err(_) => {}
        //             }
        //         }
    
        //         // position wasn't stopped
        //         // let was_position_closed =
        //         //     position_closes_col[index - 1] == 1 && current_side != Side::Nil;
        //         let was_long_closed = long_closes_col[index - 1] == 1 && current_side == Side::Buy;
        //         let was_short_closed =
        //             short_closes_col[index - 1] == 1 && current_side == Side::Sell;
    
        //         let was_position_reverted = trading_settings.signals_revert_its_opposite
        //             && (longs_col[index - 1] == 1 && current_side == Side::Sell)
        //             || (shorts_col[index - 1] == 1 && current_side == Side::Buy);
    
        //         if
        //         // was_position_closed ||
        //         was_long_closed || was_short_closed || was_position_reverted {
        //             let close_signal = if was_position_reverted {
        //                 SignalCategory::RevertPosition
        //             } else if was_long_closed {
        //                 SignalCategory::CloseLong
        //             } else if was_short_closed {
        //                 SignalCategory::CloseShort
        //             }
        //             // else if was_position_closed {
        //             //     SignalCategory::ClosePosition
        //             // }
        //             else {
        //                 SignalCategory::KeepPosition
        //             };
    
        //             let current_timestamp = start_timestamps_vec[index];
        //             let open_price = open_prices_col[index];
    
        //             match exchange.new_benchmark_close_order(
        //                 current_timestamp,
        //                 &trade.id,
        //                 open_price,
        //                 trade.open_order.clone(),
        //                 OrderStatus::Closed,
        //             ) {
        //                 Ok(close_order) => {
        //                     if close_signal != SignalCategory::RevertPosition {
        //                         let updated_trade = trade.update_trade(close_order.clone())?;
    
        //                         trade_fees.push(close_order.get_executed_order_fee());
        //                         units.push(0.0);
        //                         let (pnl, trade_returns) =
        //                             updated_trade.calculate_pnl_and_returns();
        //                         profit_and_loss.push(pnl);
        //                         returns.push(trade_returns);
        //                         let order_cost = trade.open_order.get_order_cost().unwrap();
    
        //                         balances.push(current_balance + order_cost + pnl);
        //                         positions.push(0);
        //                         actions.push(close_signal.get_column().to_string());
        //                         current_trade = None;
        //                         // current_position_signal = "";
        //                         current_peak_returns = 0.0;
        //                     } else {
        //                         let end_timestamp = end_timestamps_vec[index];
        //                         let close_price = close_prices_col[index];
    
        //                         let updated_trade = trade.update_trade(close_order.clone())?;
    
        //                         let mut total_fee = close_order.get_executed_order_fee();
        //                         let (pnl, trade_returns) =
        //                             updated_trade.calculate_pnl_and_returns();
        //                         profit_and_loss.push(pnl);
        //                         returns.push(trade_returns);
    
        //                         let order_cost = trade.open_order.get_order_cost().unwrap();
        //                         let after_close_balance = current_balance + order_cost + pnl;
    
        //                         match exchange.new_benchmark_open_order(
        //                             end_timestamp,
        //                             close_order.side,
        //                             after_close_balance,
        //                             close_price,
        //                         ) {
        //                             Ok(open_order) => {
        //                                 let open_trade: Trade = open_order.clone().into();
        //                                 total_fee += open_trade.get_executed_fees();
    
        //                                 units.push(open_order.units);
    
        //                                 let open_order_cost = open_order.get_order_cost();
        //                                 if open_order_cost.is_none() {
        //                                     panic!("open_order_cost is none {:?}", open_order);
        //                                 }
        //                                 let open_order_cost = open_order_cost.unwrap();
    
        //                                 balances.push(f64::max(
        //                                     0.0,
        //                                     after_close_balance - open_order_cost,
        //                                 ));
    
        //                                 positions.push(open_order.side.into());
        //                                 actions.push(close_signal.get_column().to_string());
        //                                 current_trade = Some(open_trade);
        //                             }
        //                             Err(_) => {
        //                                 units.push(0.0);
        //                                 balances.push(after_close_balance);
        //                                 positions.push(0);
        //                                 actions.push(
        //                                     SignalCategory::ClosePosition.get_column().to_string(),
        //                                 );
        //                                 current_trade = None;
        //                             }
        //                         }
    
        //                         trade_fees.push(total_fee);
        //                         current_peak_returns = 0.0;
        //                     }
    
        //                     continue;
        //                 }
        //                 Err(error) => {
        //                     println!("create_benchmark_close_order WARNING: {:?}", error)
        //                 }
        //             }
        //         }
    
        //         let curr_close_price = close_prices_col[index];
        //         let curr_end_timestamp = end_timestamps_vec[index];
    
        //         // TRANSACTION modifiers (stop loss, take profit) should be checked for closing positions regardless of signals
    
        //         let (_, curr_returns) =
        //             trade.calculate_current_pnl_and_returns(curr_end_timestamp, curr_close_price);
    
        //         if curr_returns > 0.0 && curr_returns > current_peak_returns {
        //             current_peak_returns = curr_returns;
        //         }
    
        //         returns.push(curr_returns);
        //     }
    
        //     trade_fees.push(0.0);
        //     units.push(current_units);
        //     profit_and_loss.push(0.0);
        //     positions.push(current_position);
        //     actions.push(SignalCategory::KeepPosition.get_column().to_string());
        //     balances.push(current_balance);
        // }
        // // if last position was taken
        // if positions.last().unwrap() != &0 {
        //     if let Some((before_last_order_index, _)) =
        //         positions // over positions vector
        //             .iter() // iterate over
        //             .enumerate() // an enumeration
        //             .rev() // of reversed positions
        //             .find(|(_, value)| value == &&0)
        //     // until it finds where value is 0
        //     {
        //         // splices results vectors to values before opening the order
    
        //         // note that even though the vector was reversed, before_last_order_index keeps being the original vector index. Thanks, Rust <3
        //         let range = before_last_order_index..dataframe_height;
        //         let zeroed_float_patch: Vec<f64> = range.clone().map(|_| 0.0 as f64).collect();
        //         let zeroed_integer_patch: Vec<i32> = range.clone().map(|_| 0 as i32).collect();
        //         let keep_position_action_patch: Vec<String> = range
        //             .clone()
        //             .map(|_| SignalCategory::KeepPosition.get_column().to_string())
        //             .collect();
    
        //         trade_fees.splice(range.clone(), zeroed_float_patch.clone());
        //         units.splice(range.clone(), zeroed_float_patch.clone());
        //         profit_and_loss.splice(range.clone(), zeroed_float_patch.clone());
    
        //         positions.splice(range.clone(), zeroed_integer_patch);
        //         actions.splice(range.clone(), keep_position_action_patch);
    
        //         let previous_balance = balances[before_last_order_index];
        //         let patch_balances: Vec<f64> =
        //             range.clone().map(|_| previous_balance as f64).collect();
        //         balances.splice(range.clone(), patch_balances);
        //         returns.splice(range.clone(), zeroed_float_patch);
        //     }
        // }
    
        // let elapsed_time = start_time.elapsed();
        // let elapsed_millis = elapsed_time.as_nanos();
        // println!(
        //     "compute_benchmark_positions => Elapsed time in nanos: {}",
        //     elapsed_millis
        // );
    
        // let trade_fee_series = Series::new("trade_fees", trade_fees);
        // let units_series = Series::new("units", units);
        // let profit_and_loss_series = Series::new("profit_and_loss", profit_and_loss);
        // let returns_series = Series::new("returns", returns);
        // let balance_series = Series::new("balance", balances);
        // let position_series = Series::new("position", positions);
        // let action_series = Series::new("action", actions);
    
        // let df = df.with_column(trade_fee_series)?;
        // let df = df.with_column(units_series)?;
        // let df = df.with_column(profit_and_loss_series)?;
        // let df = df.with_column(returns_series)?;
        // let df = df.with_column(balance_series)?;
        // let df = df.with_column(position_series)?;
        // let df = df.with_column(action_series)?;
    
        // // let path = "data/test".to_string();
        // // let file_name = "benchmark_data.csv".to_string();
        // // save_csv(path.clone(), file_name, &df, true)?;
        // // println!("@@@@@ compute_benchmark_positions {:?}", df);
    
        // let result = df.clone().lazy().select([
        //     col("start_time"),
        //     col("trade_fees"),
        //     col("units"),
        //     col("profit_and_loss"),
        //     col("returns"),
        //     col("balance"),
        //     col("position"),
        //     col("action"),
        // ]);
    
        // Ok(result)
    }
    

    fn handle_initial_strategy_data(&self, initial_strategy_df: DataFrame) -> Result<(), GlowError> {
        let benchmark_data = self.compute_benchmark_positions(initial_strategy_df)?;
        {
            let mut trading_data_lock = self.trading_data.lock().expect("trading data deadlock");
            *trading_data_lock = benchmark_data.clone();
        }
        self.benchmark_data_emitter.next(benchmark_data);
        Ok(())
    }



    fn update_trading_data(&self, updated_strategy_df: DataFrame) -> Result<DataFrame, GlowError> {
        // println!("{} update_trading_data", current_timestamp_ms());
        // missing trade_fees, units, profit_and_loss, returns, balance, position, action
        let (start_times,
            mut trades_fees,
            mut units,
            mut pnl,
            mut returns,
            mut balances,
            mut positions,
            mut actions) = get_trading_columns_values(&updated_strategy_df)?;
    
        if start_times.is_empty() {
            let error = "start_times vector is empty".to_string();
            let error = GlowError::new(String::from("Empty start times"), error);
            return Err(error);
        }
    
        let index = start_times.len() - 1;
        let previous_index = index - 1;
    
    
        let balance = self.current_balance_listener.value();
        balances[index] = Some(balance.available_to_withdraw);
        let signal = self.signal_listener.value();
        actions[index] = Some(signal.get_column());
        
    
        let exchange_binding = self.exchange_listener.value();
        let traded_symbol = &exchange_binding.get_traded_contract().symbol;
        let close_col = traded_symbol.get_close_col();

        trades_fees[index] = Some(0.0);
        units[index] = Some(0.0);
        pnl[index] = Some(0.0);
        returns[index] = Some(0.0);
        positions[index] = Some(0);

        let trade = self.current_trade_listener.value();

        if trade.is_some() {
            let current_trade = trade.unwrap();
            let trade_status = current_trade.status();
            if trade_status != TradeStatus::Cancelled && trade_status != TradeStatus::Closed {
                let current_price = &updated_strategy_df
                    .column(&close_col).unwrap()
                    .f64()?
                    .into_iter()
                    .last().unwrap()
                    .unwrap();

                let interval_start_timestamp = start_times[previous_index].unwrap();
                let interval_end_timestamp = start_times[index].unwrap();

                let (profit_and_loss, current_returns) = current_trade
                    .calculate_current_pnl_and_returns(interval_end_timestamp, *current_price);

                let interval_fee = current_trade.get_executed_fees_between_interval(
                    interval_start_timestamp,
                    interval_end_timestamp,
                );

                let current_units = current_trade.get_interval_units(interval_end_timestamp);

                trades_fees[index] = Some(interval_fee);
                units[index] = Some(current_units);
                pnl[index] = Some(profit_and_loss);
                returns[index] = Some(current_returns);
                positions[index] = Some(current_trade.open_order.side.into());
            }
        }
    
        let mut updated_strategy_df = updated_strategy_df.clone();
        updated_strategy_df
            .replace("trade_fees", Series::new("trade_fees", trades_fees))?;
        updated_strategy_df.replace("units", Series::new("units", units))?;
        updated_strategy_df
            .replace("profit_and_loss", Series::new("profit_and_loss", pnl))?;
        updated_strategy_df.replace("returns", Series::new("returns", returns))?;
        updated_strategy_df.replace("balance", Series::new("balance", balances))?;
        updated_strategy_df.replace("position", Series::new("position", positions))?;
        updated_strategy_df.replace("action", Series::new("action", actions))?;
    
        Ok(updated_strategy_df)
    }

    
    pub fn generate_last_position_signal(
        &self,
        updated_trading_df: &DataFrame
    ) -> Result<SignalCategory, GlowError> {

        let current_trade = self.current_trade_listener.value();
        let mut emitted_signal = SignalCategory::KeepPosition;

        if current_trade.is_none() {
            if check_last_index_for_signal(updated_trading_df, SignalCategory::GoLong)? {
                emitted_signal = SignalCategory::GoLong;
            } else if check_last_index_for_signal(updated_trading_df, SignalCategory::GoShort)? {
                emitted_signal = SignalCategory::GoShort;
            }
        } else {
            let current_trade = current_trade.unwrap();
            let trade_status = current_trade.get_current_position();
            if trade_status != TradeStatus::Cancelled && trade_status != TradeStatus::Closed {

            }
        }

        Ok(emitted_signal)

        // todo!()
        // let signals = [SignalCategory::GoShort, SignalCategory::GoLong, SignalCategory::CloseShort, SignalCategory::CloseLong, SignalCategory::ClosePosition];

        // let mut signals_cols = HashSet::new();
        // let trading_data_schema = updated_strategy_df.schema();

        // for signal in signals {
        //     let col = signal.get_column();
        //     if trading_data_schema.contains(col) {
        //         signals_cols.insert(col);
        //     }
        // }
        // // uses hashset to ensure no SignalCategory is double counted
        // // for signal in self.signals.clone().into_iter() {
        // //     signals_cols.insert(String::from(signal.signal_category().get_column()));
        // // }
        // let contains_short = trading_data_schema.contains(SignalCategory::GoShort.get_column());
        // let contains_long = trading_data_schema.contains(SignalCategory::GoLong.get_column());
        // let contains_short_close = trading_data_schema.contains(SignalCategory::CloseShort.get_column());
        // let contains_long_close = trading_data_schema.contains(SignalCategory::CloseLong.get_column());
    
        // let contains_position_close =
        //     signals_cols.contains(SignalCategory::ClosePosition.get_column());
    
        // let signals_filtered_df = updated_strategy_df.select(&signals_cols)?;
    
        // let mut signals_cols_map = HashMap::new();
    
        // signals_cols.clone().into_iter().for_each(|col| {
        //     signals_cols_map.insert(
        //         col.clone(),
        //         signals_filtered_df
        //             .column(&col)
        //             .unwrap()
        //             .i32()
        //             .unwrap()
        //             .into_no_null_iter()
        //             .collect::<Vec<i32>>(),
        //     );
        // });
    
        // let last_index = updated_strategy_df.height() - 1;
        // // let penultimate_index = last_index - 1;
    
        // let positions = updated_strategy_df
        //     .column("position")?
        //     .i32()
        //     .unwrap()
        //     .into_iter()
        //     .collect::<Vec<_>>();
        // let current_position = positions[last_index].unwrap();
        
    
        // // position is neutral
        // if current_position == 0 {
        //     // println!("{} | generate_last_position_signal -> last_index ={}, last start_time = {}, current_position = {}", current_timestamp_ms(), &last_index, last_start_time,  &current_position);
        //     if contains_short
        //         && signals_cols_map
        //             .get(SignalCategory::GoShort.get_column())
        //             .unwrap()[last_index]
        //             == 1
        //     {
        //         return Ok(SignalCategory::GoShort);
        //     }
    
        //     if contains_long
        //         && signals_cols_map
        //             .get(SignalCategory::GoLong.get_column())
        //             .unwrap()[last_index]
        //             == 1
        //     {
        //         return Ok(SignalCategory::GoLong);
        //     }
        // } else {
        //     // position is not closed
        //     let is_position_close = contains_position_close
        //         && signals_cols_map
        //             .get(SignalCategory::ClosePosition.get_column())
        //             .unwrap()[last_index]
        //             == 1;
    
        //     let is_long_close = current_position == 1
        //         && contains_long_close
        //         && signals_cols_map
        //             .get(SignalCategory::CloseLong.get_column())
        //             .unwrap()[last_index]
        //             == 1;
    
        //     let is_short_close = current_position == -1
        //         && contains_short_close
        //         && signals_cols_map
        //             .get(SignalCategory::CloseShort.get_column())
        //             .unwrap()[last_index]
        //             == 1;
    
        //     // println!(
        //     //     "is_short_close = {}, contains_short_close = {} signals_cols_map.get = {}",
        //     //     is_short_close,
        //     //     contains_short_close,
        //     //     signals_cols_map
        //     //         .get(SignalCategory::CloseShort.get_column())
        //     //         .unwrap()[last_index]
        //     // );
    
        //     // println!("{} | generate_last_position_signal -> last_index ={}, last start_time = {}, current_position = {}", current_timestamp_ms(), &last_index, last_start_time,  &current_position);
    
        //     if is_position_close || is_long_close || is_short_close {
        //         let close_signal = if is_position_close {
        //             SignalCategory::ClosePosition
        //         } else if is_short_close {
        //             SignalCategory::CloseShort
        //         } else if is_long_close {
        //             SignalCategory::CloseLong
        //         } else {
        //             return Ok(SignalCategory::KeepPosition);
        //         };
    
        //         return Ok(close_signal);
        //     }
        // }
    
        // Ok(SignalCategory::KeepPosition)
    }

    fn handle_updated_strategy_data(&self, updated_strategy_df: DataFrame) -> Result<(), GlowError> {
        let updated_strategy_df = self.update_trading_data(updated_strategy_df)?;

        let signal = self.generate_last_position_signal(&updated_strategy_df)?;

        {
            let mut trading_data_lock = self.trading_data.lock().expect("trading data deadlock");
            *trading_data_lock = updated_strategy_df.clone();
        }

        self.signal_listener.next(signal);

        Ok(())
    }

    fn init_strategy_data_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.strategy_data_listener.subscribe();
            while let Some(strategy_data_update) = subscription.next().await {
                match strategy_data_update {
                    StrategyDataUpdate::Initial(_) => todo!(),
                    StrategyDataUpdate::Market(_) => todo!(),
                    _ => {},
                }
            }

        })
    }

    
    pub async fn init(&self) {
        // let leverage_listener = self.leverage_listener.clone();

        // TODO: CHECK THIS QUERY
        // let leverage_change_handle = spawn(async move {
        //     let mut subscription = leverage_listener.subscribe();
        //     while let Some(leverage) = subscription.next().await {
        //         let exchange_binding = exchange_listener.value();
        //         let result = exchange_binding.set_leverage(leverage.clone()).await;
        //         match result {
        //             Ok(success) => {
        //                 if success {
        //                     let mut settings_guard = trading_settings_arc
        //                         .lock()
        //                         .expect("leverage_change_handle -> trading setting deadlock");
        //                     settings_guard.leverage = leverage;
        //                 }
        //             }
        //             Err(error) => {
        //                 println!("leverage_change_handle error {:?}", error);
        //             }
        //         }
        //     }
        // });
        self.init_balance_update_handler();
        self.init_executions_update_handler();
        self.init_order_update_handler();
        self.init_signal_handler();
        self.init_trade_update_handler();
        // self.init_trading_data_update_handler();
        
    }
    
    
}


async fn open_order(
    exchange: TraderExchangeWrapper,
    side: Side,
    available_to_withdraw: f64,
    last_price: f64,
) -> Result<(), GlowError> {
    match exchange
        .open_order(
            side,
            available_to_withdraw,
            last_price,
        )
        .await
    {
        Ok(open_order) => Ok(()),
        Err(error) => {
            let error = format!(
                "Open order error. side {:?}, last price: {:?} {:?}",
                side, last_price, error
            );
            let error = GlowError::new(String::from("Open Order Error"), error);
            Err(error)
        }
    }
}


fn add_executions_to_order_and_remove_from_temp(
    temp_executions_arc: &Arc<Mutex<Vec<Execution>>>,
    order: Order,
) -> Order {
    let mut updated_order = order.clone();
    let mut temp_executions_guard = temp_executions_arc
        .lock()
        .expect("process_last_signal -> temp_executions locked!");

    let order_uuid = &order.uuid;

    let mut pending_executions = vec![];
    let mut removed_executions_ids = vec![];

    let mut iterator = temp_executions_guard.iter();
    while let Some(execution) = iterator.next() {
        if &execution.order_uuid != "" && &execution.order_uuid == order_uuid {
            pending_executions.push(execution.clone());
            removed_executions_ids.push(execution.id.clone());
        }
    }

    if pending_executions.len() > 0 {
        updated_order = updated_order.push_executions_if_new(pending_executions);
        let filtered_temp_executions = temp_executions_guard
            .clone()
            .into_iter()
            .filter(|execution| !removed_executions_ids.contains(&execution.id))
            .collect::<Vec<Execution>>();

        *temp_executions_guard = filtered_temp_executions;
    }
    updated_order
}





