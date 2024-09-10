use common::{
    enums::{
        balance::Balance, modifiers::price_level::PriceLevel, order_action::OrderAction,
        order_status::OrderStatus, side::Side, signal_category::SignalCategory,
        trade_status::TradeStatus, trading_data_update::TradingDataUpdate,
    },
    functions::{
        check_last_index_for_signal, current_datetime, current_timestamp_ms, get_price_columns,
        get_signal_col_values, get_trading_columns_values,
    },
    structs::{BehaviorSubject, Execution, Order, Trade, TradingSettings},
    traits::exchange::{BenchmarkExchange, TraderExchange, TraderHelper},
};
use exchanges::enums::TraderExchangeWrapper;
use glow_error::GlowError;
use polars::prelude::*;
use std::{
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct Trader {
    current_balance_listener: BehaviorSubject<Balance>,
    current_trade_listener: BehaviorSubject<Option<Trade>>,
    executions_update_listener: BehaviorSubject<Vec<Execution>>,
    order_update_listener: BehaviorSubject<OrderAction>,
    pub performance_data_emitter: BehaviorSubject<TradingDataUpdate>,
    signal_listener: BehaviorSubject<SignalCategory>,
    strategy_data_listener: BehaviorSubject<TradingDataUpdate>,
    temp_executions: Arc<Mutex<Vec<Execution>>>,
    trader_exchange: TraderExchangeWrapper,
    trading_data: Arc<Mutex<DataFrame>>,
    trading_data_klines_limit: Arc<RwLock<u32>>,
}

impl Trader {
    fn get_listeners(
        trader_exchange: &TraderExchangeWrapper,
    ) -> (
        &BehaviorSubject<Balance>,
        &BehaviorSubject<Vec<Execution>>,
        &BehaviorSubject<OrderAction>,
        &BehaviorSubject<Option<Trade>>,
    ) {
        let current_balance_listener = trader_exchange.get_balance_update_emitter();
        let executions_update_listener = trader_exchange.get_executions_update_emitter();
        let order_update_listener = trader_exchange.get_order_update_emitter();
        let current_trade_listener = trader_exchange.get_trade_update_emitter();

        (
            current_balance_listener,
            executions_update_listener,
            order_update_listener,
            current_trade_listener,
        )
    }

    pub fn new(
        strategy_data_listener: &BehaviorSubject<TradingDataUpdate>,
        trader_exchange: TraderExchangeWrapper,
        trading_data: &Arc<Mutex<DataFrame>>,
        trading_data_klines_limit: &Arc<RwLock<u32>>,
    ) -> Trader {
        let performance_data_emitter = BehaviorSubject::new(TradingDataUpdate::default());
        let (
            current_balance_listener,
            executions_update_listener,
            order_update_listener,
            current_trade_listener,
        ) = Self::get_listeners(&trader_exchange);
        Trader {
            current_balance_listener: current_balance_listener.clone(),
            current_trade_listener: current_trade_listener.clone(),
            executions_update_listener: executions_update_listener.clone(),
            order_update_listener: order_update_listener.clone(),
            performance_data_emitter: performance_data_emitter.clone(),
            signal_listener: BehaviorSubject::new(SignalCategory::default()),
            temp_executions: Arc::new(Mutex::new(Vec::new())),
            strategy_data_listener: strategy_data_listener.clone(),
            trader_exchange,
            trading_data: trading_data.clone(),
            trading_data_klines_limit: trading_data_klines_limit.clone(),
        }
    }

    pub fn patch_settings(&mut self, trading_settings: &TradingSettings) {
        self.trader_exchange.patch_settings(trading_settings);
    }

    fn get_trading_data(&self) -> Result<DataFrame, GlowError> {
        let trading_data: DataFrame;
        {
            let lock = self.trading_data.lock().expect("trading data deadlock");
            trading_data = lock.clone();
        }
        Ok(trading_data)
    }

    fn update_trading_data(&self, payload: DataFrame) -> Result<(), GlowError> {
        {
            let mut lock = self
                .trading_data
                .lock()
                .expect("update trading data deadlock");
            *lock = payload;
        }
        Ok(())
    }

    fn get_temp_executions(&self) -> Result<Vec<Execution>, GlowError> {
        let temp_executions: Vec<Execution>;
        {
            let lock = self
                .temp_executions
                .lock()
                .expect("temp_executions deadlock");
            temp_executions = lock.clone();
        }
        Ok(temp_executions)
    }

    fn push_to_temp_executions(&self, payload: Vec<Execution>) -> Result<(), GlowError> {
        {
            let mut lock = self
                .temp_executions
                .lock()
                .expect("update temp_executions deadlock");
            let mut updated_value = lock.clone();
            updated_value.extend(payload);
            *lock = updated_value;
        }
        Ok(())
    }

    async fn process_last_signal(&self, signal: SignalCategory) -> Result<(), GlowError> {
        let current_trade = self.current_trade_listener.value();
        let traded_symbol = self.trader_exchange.get_traded_symbol();
        let close_col = traded_symbol.get_close_col();
        let trading_data = self.get_trading_data()?;
        // TODO: check if this can be received via param
        let last_price = trading_data
            .column(&close_col)?
            .f64()?
            .into_no_null_iter()
            .last()
            .expect("process_last_signal -> SignalCategory::GoLong -> missing last price");

        if current_trade.is_none() {
            let available_to_withdraw = self.current_balance_listener.value().available_to_withdraw;
            return Ok(open_order(
                &self.trader_exchange,
                signal.into(),
                available_to_withdraw,
                last_price,
            )
            .await?);
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
                match self.trader_exchange
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
                                    &self.trader_exchange,
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
            (TradeStatus::PartiallyOpen | TradeStatus::PendingCloseOrder, SignalCategory::CloseLong, Side::Buy) |
            (TradeStatus::PartiallyOpen | TradeStatus::PendingCloseOrder, SignalCategory::CloseShort, Side::Sell) |
            (TradeStatus::PartiallyOpen | TradeStatus::PendingCloseOrder, SignalCategory::ClosePosition, _)
             => {
                if current_trade_status == &TradeStatus::PartiallyOpen {
                    let mut open_order = current_trade.open_order.clone();
                    let left_units = open_order.get_executed_quantity() - open_order.units;
                    let updated_units = Some(left_units);
                    let updated_price = None;
                    let updated_stop_loss_price = None;
                    let updated_take_profit_price = None;
                    let amend_result = self.trader_exchange
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

                match self.trader_exchange
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

    fn init_signal_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.signal_listener.subscribe();
            while let Some(signal) = subscription.next().await {
                if signal == SignalCategory::KeepPosition {
                    continue;
                }
                match trader.process_last_signal(signal).await {
                    Ok(()) => {}
                    Err(error) => {
                        println!("process_last_signal error {:?}", error);
                    }
                }
            }
        })
    }

    // fn init_balance_update_handler(&self) -> JoinHandle<()> {
    //     let trader = self.clone();
    //     spawn(async move {
    //         let mut subscription = trader.update_balance_listener.subscribe();
    //         while let Some(balance_update) = subscription.next().await {
    //             if let Some(balance) = balance_update {
    //                 trader.current_balance_listener.next(balance)
    //             }
    //         }
    //     })
    // }

    fn add_executions_to_order_and_remove_from_temp(&self, order: Order) -> Order {
        // let mut updated_order = order.clone();
        let mut temp_executions_guard = self
            .temp_executions
            .lock()
            .expect("process_last_signal -> temp_executions locked!");

        if temp_executions_guard.len() <= 0 {
            return order;
        }

        let order_uuid = &order.uuid;
        let mut pending_executions = vec![];
        let mut removed_executions_ids = vec![];

        temp_executions_guard.iter().for_each(|execution| {
            if &execution.order_uuid != "" && &execution.order_uuid == order_uuid {
                pending_executions.push(execution.clone());
                removed_executions_ids.push(execution.id.clone());
            }
        });

        if pending_executions.len() <= 0 {
            return order;
        }

        let updated_order = order.push_executions_if_new(pending_executions);
        let filtered_temp_executions = temp_executions_guard
            .clone()
            .into_iter()
            .filter(|execution| !removed_executions_ids.contains(&execution.id))
            .collect::<Vec<Execution>>();

        *temp_executions_guard = filtered_temp_executions;
        updated_order
    }

    fn init_order_update_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.order_update_listener.subscribe();
            while let Some(order_action) = subscription.next().await {
                let current_trade = trader.current_trade_listener.value();
                match order_action.clone() {
                    OrderAction::Update(mut updated_order)
                    | OrderAction::Stop(mut updated_order) => {
                        updated_order =
                            trader.add_executions_to_order_and_remove_from_temp(updated_order);

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
                            let new_trade = Trade::new(updated_order, None);
                            trader.current_trade_listener.next(Some(new_trade));
                            continue;
                        }
                        let current_trade = current_trade.unwrap();
                        match current_trade.update_trade(updated_order.clone()) {
                            Ok(updated_trade) => {
                                // println!("match trade, updated {:?}", &updated_trade);
                                if let OrderAction::Stop(_) = order_action {
                                    let (pnl, returns) = updated_trade.calculate_pnl_and_returns();
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
                    _ => {}
                }
            }
        })
    }

    fn init_executions_update_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.executions_update_listener.subscribe();
            while let Some(latest_executions) = subscription.next().await {
                if latest_executions.len() <= 0 {
                    continue;
                }

                match trader.push_to_temp_executions(latest_executions) {
                    Ok(_) => {}
                    Err(error) => {
                        println!("push_to_temp_executions error {:?}", error);
                    }
                }
            }
        })
    }

    fn on_close_trade_update_trading_data(&self) -> Result<(), GlowError> {
        // println!("{} on_close_trade_update_trading_data", current_timestamp_ms());
        let current_trade = self.current_trade_listener.value();
        if current_trade.is_none() {
            let error = GlowError::new_str("Invalid Trade Status", "Trade is None");
            return Err(error);
        }
        let current_trade = current_trade.unwrap();
        let trade_status = current_trade.status();
        if trade_status != TradeStatus::Cancelled && trade_status != TradeStatus::Closed {
            let error = GlowError::new_str(
                "Invalid Trade Status",
                "Status should've been TradeStatus::Closed or TradeStatus::Cancelled",
            );
            return Err(error);
        }

        // missing trade_fees, units, profit_and_loss, returns, balance, position, action
        let trading_data = self.get_trading_data()?;

        let (
            start_times,
            mut fees_col,
            mut units,
            mut pnl_col,
            mut returns_col,
            mut balances,
            mut positions,
            mut actions,
        ) = get_trading_columns_values(&trading_data)?;

        let index = start_times.len() - 1;
        let balance = self.current_balance_listener.value();
        let signal = self.signal_listener.value();

        let (fees, pnl, returns) = if trade_status == TradeStatus::Cancelled {
            (0.0, 0.0, 0.0)
        } else {
            let start_timestamp = start_times[index].expect(
                "on_close_trade_update_trading_data -> TradeStatus::Closed arm -> interval_start_timestamp unwrap",
            );
            let end_timestamp = current_timestamp_ms() as i64;
            let (pnl, returns) = current_trade.calculate_pnl_and_returns();
            let fees =
                current_trade.get_executed_fees_between_interval(start_timestamp, end_timestamp);
            (fees, pnl, returns)
        };
        fees_col[index] = Some(fees);
        units[index] = Some(0.0);
        pnl_col[index] = Some(pnl);
        returns_col[index] = Some(returns);
        positions[index] = Some(0);
        balances[index] = Some(balance.available_to_withdraw);
        actions[index] = Some(signal.get_column());

        let mut trading_data = trading_data.clone();
        trading_data.replace("trade_fees", Series::new("trade_fees", fees_col))?;
        trading_data.replace("units", Series::new("units", units))?;
        trading_data.replace("profit_and_loss", Series::new("profit_and_loss", pnl_col))?;
        trading_data.replace("returns", Series::new("returns", returns_col))?;
        trading_data.replace("balance", Series::new("balance", balances))?;
        trading_data.replace("position", Series::new("position", positions))?;
        trading_data.replace("action", Series::new("action", actions))?;

        self.update_trading_data(trading_data)?;

        self.current_trade_listener.next(None);
        Ok(())
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
                if trade_status != TradeStatus::Cancelled && trade_status != TradeStatus::Closed {
                    continue;
                }

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

                match trader.on_close_trade_update_trading_data() {
                    Ok(_) => {}
                    Err(error) => println!("on_close_trade_update_trading_data error {:?}", error),
                }
            }
        })
    }

    fn compute_benchmark_positions(
        &self,
        initial_strategy_df: DataFrame,
    ) -> Result<DataFrame, GlowError> {
        // let data = data.to_owned();
        // TODO: TRY TO IMPLEMENT THIS USING LAZYFRAMES
        let perf_start = Instant::now();

        let mut df = initial_strategy_df;
        let df_height = df.height();

        let start_timestamps = df
            .column("start_time")
            .unwrap()
            .datetime()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<i64>>();

        let end_timestamps = start_timestamps
            .clone()
            .into_iter()
            .map(|start_timestamp| start_timestamp - 1)
            .collect::<Vec<i64>>();

        let traded_symbol = self.trader_exchange.get_traded_symbol();
        let (opens, highs, lows, closes) = get_price_columns(&df, &traded_symbol)?;
        let shorts = get_signal_col_values(&df, SignalCategory::GoShort)?;
        let longs = get_signal_col_values(&df, SignalCategory::GoShort)?;
        let close_shorts = get_signal_col_values(&df, SignalCategory::CloseShort)?;
        let close_longs = get_signal_col_values(&df, SignalCategory::CloseLong)?;

        let mut trade_fees = vec![0.0];
        let mut units = vec![0.0];
        let mut profit_and_loss = vec![0.0];
        let mut returns = vec![0.0];
        let mut balances = vec![100.0];
        let mut positions = vec![0];
        let mut actions = vec![SignalCategory::KeepPosition.get_column().to_owned()];

        let trading_settings = self.trader_exchange.get_trading_settings();

        let leverage_factor = self.trader_exchange.get_leverage_factor();
        let has_leverage = leverage_factor > 1.0;

        let price_level_modifier_map_binding = trading_settings.price_level_modifier_map.clone();
        let stop_loss: Option<&PriceLevel> = price_level_modifier_map_binding.get("sl");
        let take_profit = price_level_modifier_map_binding.get("tp");
        let trailing_stop_loss = price_level_modifier_map_binding.get("tsl");
        let should_check_price_modifiers = has_leverage
            || stop_loss.is_some()
            || take_profit.is_some()
            || trailing_stop_loss.is_some();

        let mut current_trade: Option<Trade> = None;
        // let mut current_peak_returns = 0.0;
        let mut current_min_price_threshold = None;
        let mut current_max_price_threshold = None;

        // need to be updated
        // trade_fees, units, profit_and_loss, returns, balances, positions, actions

        for index in 0..df_height {
            if index == 0 {
                continue;
            }

            let current_position = positions[index - 1];
            let current_units = units[index - 1];
            let current_balance = balances[index - 1];

            let default_results = (
                0.0,
                current_units,
                0.0,
                0.0,
                current_balance,
                current_position,
                SignalCategory::KeepPosition.get_column().to_owned(),
            );

            let (fee, unit, pnl, trade_returns, balance, position, action) = if current_position
                == 0
            {
                let should_short = shorts[index - 1] == 1;
                let should_long = longs[index - 1] == 1;
                if should_short || should_long {
                    let start_timestamp = start_timestamps[index];
                    let end_timestamp = end_timestamps[index];
                    let open_price = opens[index];
                    let close_price = closes[index];

                    match self.trader_exchange.new_benchmark_open_order(
                        start_timestamp,
                        if should_short { Side::Sell } else { Side::Buy },
                        current_balance,
                        open_price,
                    ) {
                        Ok(open_order) => {
                            let open_trade: Trade = open_order.clone().into();
                            let fees = open_trade.get_executed_fees();
                            let (pnl, trade_returns) = open_trade
                                .calculate_current_pnl_and_returns(end_timestamp, close_price);
                            let open_order_cost =
                                open_order.get_order_cost().expect("order to have cost");
                            (current_min_price_threshold, current_max_price_threshold) =
                                open_trade.get_threshold_prices();
                            current_trade = Some(open_trade);
                            (
                                fees,
                                open_order.units,
                                pnl,
                                trade_returns,
                                f64::max(0.0, current_balance - open_order_cost),
                                open_order.side.into(),
                                (if should_short {
                                    SignalCategory::GoShort
                                } else {
                                    SignalCategory::GoLong
                                })
                                .get_column()
                                .to_owned(),
                            )
                        }
                        Err(error) => {
                            println!("create_new_benchmark_open_order error {:?}", error);
                            default_results
                        }
                    }
                } else {
                    default_results
                }
            } else {
                let trade = current_trade.clone().unwrap();
                let current_side = trade.open_order.side;
                let stopped_result = if should_check_price_modifiers {
                    let min_price = lows[index];
                    let max_price = highs[index];
                    let binds_on_min_price =
                        min_price <= current_min_price_threshold.unwrap_or_default();
                    let binds_on_max_price =
                        max_price >= current_max_price_threshold.unwrap_or_default();
                    if binds_on_min_price || binds_on_max_price {
                        // let prev_close_price = closes[index - 1];
                        let prev_end_timestamp = end_timestamps[index - 1];
                        match self.trader_exchange.close_benchmark_trade_on_binding_price(
                            &trade,
                            prev_end_timestamp,
                            if binds_on_min_price {
                                current_min_price_threshold.unwrap()
                            } else {
                                current_max_price_threshold.unwrap()
                            },
                        ) {
                            Ok(closed_trade) => {
                                let close_order = closed_trade.clone().close_order.unwrap();
                                let (pnl, trade_returns) = closed_trade.calculate_pnl_and_returns();
                                let order_cost = closed_trade.open_order.get_order_cost().unwrap();
                                let action = match close_order.status {
                                    OrderStatus::StoppedBR => SignalCategory::LeverageBankrupcty,
                                    OrderStatus::StoppedSL => SignalCategory::StopLoss,
                                    OrderStatus::StoppedTP => SignalCategory::TakeProfit,
                                    // OrderStatus::StoppedTSL => {
                                    //     SignalCategory::TrailingStopLoss
                                    // }
                                    _ => SignalCategory::KeepPosition,
                                };
                                (current_min_price_threshold, current_max_price_threshold) =
                                    (None, None);
                                current_trade = None;
                                Some((
                                    close_order.get_executed_order_fee(),
                                    0.0,
                                    pnl,
                                    trade_returns,
                                    current_balance + order_cost + pnl,
                                    0,
                                    action.get_column().to_owned(),
                                ))
                            }
                            Err(error) => {
                                println!(
                                    "close_benchmark_trade_on_binding_price error {:?}",
                                    error
                                );
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(stopped_result) = stopped_result {
                    stopped_result
                } else {
                    // position wasn't stopped
                    let was_short_closed =
                        close_shorts[index - 1] == 1 && current_side == Side::Sell;
                    let was_long_closed = close_longs[index - 1] == 1 && current_side == Side::Buy;

                    if was_short_closed || was_long_closed {
                        let current_timestamp = start_timestamps[index];
                        let open_price = opens[index];

                        match self.trader_exchange.new_benchmark_close_order(
                            current_timestamp,
                            &trade.id,
                            open_price,
                            trade.open_order.clone(),
                            OrderStatus::Closed,
                        ) {
                            Ok(close_order) => {
                                let updated_trade = trade.update_trade(close_order.clone())?;
                                let (pnl, trade_returns) =
                                    updated_trade.calculate_pnl_and_returns();
                                let order_cost = trade.open_order.get_order_cost().unwrap();

                                current_trade = None;
                                (
                                    close_order.get_executed_order_fee(),
                                    0.0,
                                    pnl,
                                    trade_returns,
                                    current_balance + order_cost + pnl,
                                    0,
                                    (if was_short_closed {
                                        SignalCategory::CloseShort
                                    } else {
                                        SignalCategory::CloseLong
                                    })
                                    .get_column()
                                    .to_owned(),
                                )
                            }
                            Err(error) => {
                                println!("create_benchmark_close_order WARNING: {:?}", error);
                                default_results
                            }
                        }
                    } else {
                        default_results
                    }
                }
            };

            trade_fees.push(fee);
            units.push(unit);
            profit_and_loss.push(pnl);
            returns.push(trade_returns);
            balances.push(balance);
            positions.push(position);
            actions.push(action);
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
                let range = before_last_order_index..df_height;
                let zeroed_float_patch: Vec<f64> = range.clone().map(|_| 0.0 as f64).collect();
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
                let patch_balances: Vec<f64> =
                    range.clone().map(|_| previous_balance as f64).collect();
                balances.splice(range.clone(), patch_balances);
                returns.splice(range.clone(), zeroed_float_patch);
            }
        }

        let trade_fee_series = Series::new("trade_fees", trade_fees);
        let units_series = Series::new("units", units);
        let profit_and_loss_series = Series::new("profit_and_loss", profit_and_loss);
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

        let elapsed_time = perf_start.elapsed();
        let elapsed_millis = elapsed_time.as_nanos();
        println!(
            "compute_benchmark_positions => Elapsed time in nanos: {}",
            elapsed_millis
        );

        Ok(df.clone())
    }

    fn handle_initial_strategy_data(
        &self,
        initial_strategy_df: DataFrame,
    ) -> Result<(), GlowError> {
        let benchmark_data = self.compute_benchmark_positions(initial_strategy_df)?;
        self.update_trading_data(benchmark_data.clone())?;
        let trading_data_update = TradingDataUpdate::Initial(benchmark_data);
        self.performance_data_emitter.next(trading_data_update);
        Ok(())
    }

    fn update_trading_columns(
        &self,
        updated_strategy_df: DataFrame,
    ) -> Result<DataFrame, GlowError> {
        // println!("{} update_trading_data", current_timestamp_ms());
        // missing trade_fees, units, profit_and_loss, returns, balance, position, action
        let (
            start_times,
            mut trades_fees,
            mut units,
            mut pnl,
            mut returns,
            mut balances,
            mut positions,
            mut actions,
        ) = get_trading_columns_values(&updated_strategy_df)?;

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

        let traded_symbol = self.trader_exchange.get_traded_contract().symbol;
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
                    .column(&close_col)
                    .unwrap()
                    .f64()?
                    .into_iter()
                    .last()
                    .unwrap()
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
        updated_strategy_df.replace("trade_fees", Series::new("trade_fees", trades_fees))?;
        updated_strategy_df.replace("units", Series::new("units", units))?;
        updated_strategy_df.replace("profit_and_loss", Series::new("profit_and_loss", pnl))?;
        updated_strategy_df.replace("returns", Series::new("returns", returns))?;
        updated_strategy_df.replace("balance", Series::new("balance", balances))?;
        updated_strategy_df.replace("position", Series::new("position", positions))?;
        updated_strategy_df.replace("action", Series::new("action", actions))?;

        Ok(updated_strategy_df)
    }

    pub fn generate_last_position_signal(
        &self,
        trading_data_df: &DataFrame,
    ) -> Result<SignalCategory, GlowError> {
        let current_trade = self.current_trade_listener.value();
        let mut emitted_signal = SignalCategory::KeepPosition;

        if current_trade.is_none() {
            if check_last_index_for_signal(trading_data_df, SignalCategory::GoLong)? {
                emitted_signal = SignalCategory::GoLong;
            } else if check_last_index_for_signal(trading_data_df, SignalCategory::GoShort)? {
                emitted_signal = SignalCategory::GoShort;
            }
        } else {
            let current_trade = current_trade.unwrap();
            let trade_status = current_trade.status();
            if trade_status != TradeStatus::Cancelled && trade_status != TradeStatus::Closed {
                let current_trade_side = current_trade.open_order.side;
                if current_trade_side == Side::Buy
                    && check_last_index_for_signal(trading_data_df, SignalCategory::CloseLong)?
                {
                    emitted_signal = SignalCategory::CloseLong;
                } else if current_trade_side == Side::Sell
                    && check_last_index_for_signal(trading_data_df, SignalCategory::CloseShort)?
                {
                    emitted_signal = SignalCategory::CloseShort;
                }
            }
        }
        Ok(emitted_signal)
    }

    fn clean_temp_executions(&self) -> Result<(), GlowError> {
        let current_trade = self.current_trade_listener.value();
        if current_trade.is_none() {
            return Ok(());
        }
        let current_trade = current_trade.unwrap();
        let mut temp_executions_guard = self
            .temp_executions
            .lock()
            .expect("TradingDataUpdate::CleanUp -> temp_executions deadlock");
        if temp_executions_guard.len() <= 0 {
            return Ok(());
        }
        let open_order_uuid = &current_trade.open_order.uuid;
        let close_order_uuid = &current_trade.close_order.clone().unwrap_or_default().uuid;
        let mut pending_executions = vec![];
        let mut removed_executions_ids = vec![];
        for execution in temp_executions_guard.iter() {
            if &execution.order_uuid == open_order_uuid
                || close_order_uuid != "" && &execution.order_uuid == close_order_uuid
            {
                pending_executions.push(execution.clone());
                removed_executions_ids.push(execution.id.clone());
            }
        }
        if pending_executions.len() <= 0 {
            return Ok(());
        }
        let updated_trade = current_trade
            .update_executions(pending_executions)
            .expect("TradingDataUpdate::CleanUp update_executions unwrap");
        if updated_trade.is_none() {
            return Ok(());
        }
        let updated_trade = updated_trade.unwrap();
        self.current_trade_listener.next(Some(updated_trade));
        let filtered_temp_executions = temp_executions_guard
            .clone()
            .into_iter()
            .filter(|execution| !removed_executions_ids.contains(&execution.id))
            .collect::<Vec<Execution>>();
        *temp_executions_guard = filtered_temp_executions;
        Ok(())
    }

    fn clean_trading_data(&self, trading_data: DataFrame) -> Result<DataFrame, GlowError> {
        let trading_data_klines_limit = self.trading_data_klines_limit.read().unwrap();
        let trading_data_klines_limit = trading_data_klines_limit.clone();
        let trading_data = trading_data.tail(Some(trading_data_klines_limit as usize));

        Ok(trading_data)
    }

    fn handle_updated_strategy_data(
        &self,
        updated_strategy_df: DataFrame,
    ) -> Result<(), GlowError> {
        // updates trading columns with latest indicators/signals
        let updated_df = self.update_trading_columns(updated_strategy_df)?;
        // derives latest signal from them
        let signal = self.generate_last_position_signal(&updated_df)?;
        // emits it.
        self.signal_listener.next(signal);
        // cleans trade executions
        self.clean_temp_executions()?;
        // then data
        let updated_strategy_df = self.clean_trading_data(updated_df)?;
        self.update_trading_data(updated_strategy_df)?;
        Ok(())
    }

    fn init_strategy_data_handler(&self) -> JoinHandle<()> {
        let trader = self.clone();
        spawn(async move {
            let mut subscription = trader.strategy_data_listener.subscribe();
            while let Some(strategy_data_update) = subscription.next().await {
                let result = match strategy_data_update {
                    TradingDataUpdate::Initial(initial_strategy_df) => {
                        trader.handle_initial_strategy_data(initial_strategy_df)
                    }
                    TradingDataUpdate::Market(updated_strategy_df) => {
                        trader.handle_updated_strategy_data(updated_strategy_df)
                    }
                    _ => Ok(()),
                };

                if result.is_err() {
                    println!("init_strategy_data_handler error {:?}", result);
                }
            }
        })
    }

    pub async fn init(&self) {
        // let leverage_listener = self.leverage_listener.clone();

        // TODO: This query should be run at trader exchange level, same as balance
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
        self.init_strategy_data_handler();
        // self.init_balance_update_handler();
        self.init_executions_update_handler();
        self.init_order_update_handler();
        self.init_signal_handler();
        self.init_trade_update_handler();
        // self.init_trading_data_update_handler();
    }
}

async fn open_order(
    exchange: &TraderExchangeWrapper,
    side: Side,
    available_to_withdraw: f64,
    last_price: f64,
) -> Result<(), GlowError> {
    match exchange
        .open_order(side, available_to_withdraw, last_price)
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
