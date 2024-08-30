use std::sync::{Arc, Mutex};
use common::{enums::{balance::Balance, modifiers::{leverage::Leverage, price_level::PriceLevel}, order_action::OrderAction, side::Side, signal_category::SignalCategory, trade_status::TradeStatus, trading_data_update::TradingDataUpdate}, functions::{current_datetime, current_timestamp_ms, get_symbol_close_col}, structs::{BehaviorSubject, Execution, Order, Trade, TradingSettings}};
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
    pub current_balance_listener: BehaviorSubject<Balance>,
    pub current_trade_listener: BehaviorSubject<Option<Trade>>,

    pub exchange_socket_error_arc: Arc<Mutex<Option<i64>>>,
    pub exchange_listener: BehaviorSubject<TraderExchangeWrapper>,

    pub signal_listener: BehaviorSubject<Option<SignalCategory>>,
    pub temp_executions_arc: Arc<Mutex<Vec<Execution>>>,
    
    pub trading_settings_arc: Arc<Mutex<TradingSettings>>,
    pub trading_data_listener: BehaviorSubject<DataFrame>,
    pub trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
    
    pub update_balance_listener: BehaviorSubject<Option<Balance>>,
    pub update_order_listener: BehaviorSubject<Option<OrderAction>>,
    pub update_executions_listener: BehaviorSubject<Vec<Execution>>,
}

impl Trader {
    pub fn new(
        current_balance_listener: &BehaviorSubject<Balance>,
        current_trade_listener: &BehaviorSubject<Option<Trade>>,

        exchange_socket_error_arc: &Arc<Mutex<Option<i64>>>,
        exchange_listener: &BehaviorSubject<TraderExchangeWrapper>,
        
        signal_listener: &BehaviorSubject<Option<SignalCategory>>,
        
        trading_settings_arc: &Arc<Mutex<TradingSettings>>,
        trading_data_listener: &BehaviorSubject<DataFrame>,
        trading_data_update_listener: &BehaviorSubject<TradingDataUpdate>,

        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        
    ) -> Trader {
        Trader {
            current_balance_listener: current_balance_listener.clone(),
            current_trade_listener: current_trade_listener.clone(),

            exchange_socket_error_arc: exchange_socket_error_arc.clone(),
            exchange_listener: exchange_listener.clone(),
            
            signal_listener: signal_listener.clone(),
            temp_executions_arc: Arc::new(Mutex::new(Vec::new())),
            
            trading_settings_arc: trading_settings_arc.clone(),
            update_balance_listener: update_balance_listener.clone(),
            update_order_listener: update_order_listener.clone(),
            update_executions_listener: update_executions_listener.clone(),
            trading_data_update_listener: trading_data_update_listener.clone(),
            trading_data_listener: trading_data_listener.clone(),
            
        }
    }

    async fn process_last_signal(
        &self,
        signal: SignalCategory,

    ) -> Result<(), GlowError> {
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
                if signal.is_none() || signal == Some(SignalCategory::KeepPosition) {
                    continue;
                }
                match trader.process_last_signal(
                    signal.unwrap(),
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
        let series_binding = strategy_updated_data.columns([
            "start_time",
            "trade_fees",
            "units",
            "profit_and_loss",
            "returns",
            "balance",
            "position",
            "action",
        ])?;
    
        let mut series = series_binding.iter();
    
        let start_times_vec: Vec<Option<i64>> = series
            .next()
            .expect("on_close_update_trading_data -> start_time .next error")
            .datetime()
            .expect("on_close_update_trading_data -> start_time .f64 unwrap error")
            .into_iter()
            .collect();
    
        let mut trades_fees_vec: Vec<Option<f64>> = series
            .next()
            .expect("on_close_update_trading_data -> trades_fees_vec .next error")
            .f64()
            .expect("on_close_update_trading_data -> trades_fees_vec .f64 unwrap error")
            .into_iter()
            .collect();
    
        let mut units_vec: Vec<Option<f64>> = series
            .next()
            .expect("on_close_update_trading_data -> units_vec .next error")
            .f64()
            .expect("on_close_update_trading_data -> units_vec .f64 unwrap error")
            .into_iter()
            .collect();
    
        let mut pnl_vec: Vec<Option<f64>> = series
            .next()
            .expect("on_close_update_trading_data pnl_vec .next error")
            .f64()
            .expect("on_close_update_trading_data pnl_vec .f64 unwrap error")
            .into_iter()
            .collect();
    
        let mut returns_vec: Vec<Option<f64>> = series
            .next()
            .expect("on_close_update_trading_data returns_vec .next error")
            .f64()
            .expect("on_close_update_trading_data returns_vec .f64 unwrap error")
            .into_iter()
            .collect();
    
        let mut balances_vec: Vec<Option<f64>> = series
            .next()
            .expect("on_close_update_trading_data -> balances_vec .next error")
            .f64()
            .expect("on_close_update_trading_data -> balances_vec .f64 unwrap error")
            .into_iter()
            .collect();
    
        let mut positions_vec: Vec<Option<i32>> = series
            .next()
            .expect("on_close_update_trading_data -> positions_vec .next error")
            .i32()
            .expect("on_close_update_trading_data -> positions_vec .i32 unwrap error")
            .into_iter()
            .collect();
    
        let mut actions_vec: Vec<Option<&str>> = series
            .next()
            .expect("on_close_update_trading_data -> actions_vec .next error")
            .utf8()
            .expect("on_close_update_trading_data -> actions_vec .utf8 unwrap error")
            .into_iter()
            .collect();
    
        if start_times_vec.is_empty() {
            let error = "start_times vector is empty".to_string();
            let error = GlowError::new(String::from("Empty start times"), error);
            return Err(error);
        }
    
        let index = start_times_vec.len() - 1;
    
        let balance = self.current_balance_listener.value();
        balances_vec[index] = Some(balance.available_to_withdraw);
        let signal = self.signal_listener.value().unwrap_or_default();
        actions_vec[index] = Some(signal.get_column());
        let trade = self.current_trade_listener.value();
    
        let mut result = None;
        match trade {
            Some(current_trade) => {
                let trade_status = current_trade.status();
                match trade_status {
                    TradeStatus::Cancelled | TradeStatus::Closed => {
                        if trade_status == TradeStatus::Cancelled {
                            trades_fees_vec[index] = Some(0.0);
                            units_vec[index] = Some(0.0);
                            pnl_vec[index] = Some(0.0);
                            returns_vec[index] = Some(0.0);
                            positions_vec[index] = Some(0);
                        } else {
                            let interval_start_timestamp =  start_times_vec[index]
                                    .expect("update_trading_data -> TradeStatus::Closed arm -> interval_start_timestamp unwrap");
                            let interval_end_timestamp = current_timestamp_ms() as i64;
    
                            let (profit_and_loss, current_returns) =
                                current_trade.calculate_pnl_and_returns();
    
                            let interval_fee = current_trade.get_executed_fees_between_interval(
                                interval_start_timestamp,
                                interval_end_timestamp,
                            );
    
                            trades_fees_vec[index] = Some(interval_fee);
                            units_vec[index] = Some(0.0);
                            pnl_vec[index] = Some(profit_and_loss);
                            returns_vec[index] = Some(current_returns);
                            positions_vec[index] = Some(0);
                        }
    
                        // updates df
                        strategy_updated_data_clone
                            .replace("trade_fees", Series::new("trade_fees", trades_fees_vec))?;
                        strategy_updated_data_clone
                            .replace("units", Series::new("units", units_vec))?;
                        strategy_updated_data_clone
                            .replace("profit_and_loss", Series::new("profit_and_loss", pnl_vec))?;
                        strategy_updated_data_clone
                            .replace("returns", Series::new("returns", returns_vec))?;
                        strategy_updated_data_clone
                            .replace("balance", Series::new("balance", balances_vec))?;
                        strategy_updated_data_clone
                            .replace("position", Series::new("position", positions_vec))?;
                        strategy_updated_data_clone
                            .replace("action", Series::new("action", actions_vec))?;
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
    
                    let trading_data = trader.trading_data_listener.value();
                    let updated_trading_data = trader.on_close_update_trading_data(
                        trading_data
                    )
                    .expect(
                        "get_current_trade_update_handle -> on_close_update_trading_data unwrap failed",
                    );
                    if updated_trading_data.is_some() {
                        let updated_trading_data = updated_trading_data.unwrap();
                        trader.trading_data_listener.next(updated_trading_data);
                    }
    
                    trader.current_trade_listener.next(None);
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





