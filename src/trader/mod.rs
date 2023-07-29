use self::models::modifiers::Leverage;
use self::models::signal::SignalCategory;
use self::{errors::Error, models::behavior_subject::BehaviorSubject};
use enums::log_level::LogLevel;
use futures_util::StreamExt;
use models::{market_data::MarketDataFeed, performance::Performance, strategy::Strategy};
use polars::prelude::*;
use serde::Deserialize;
use std::sync::{Arc, Mutex};

mod constants;
pub mod contracts;
pub mod enums;
pub mod errors;
pub mod exchanges;
mod functions;
pub mod indicators;
pub mod models;
pub mod signals;
#[derive(Clone)]
pub struct Trader {
    pub symbols: [String; 2],
    pub units: i64,
    pub initial_balance: f64,
    pub current_balance: BehaviorSubject<f64>,
    pub market_data_feed: MarketDataFeed,
    pub strategy_arc: Arc<Mutex<Strategy>>,
    pub performance_arc: Arc<Mutex<Performance>>,
    pub signal_generator: BehaviorSubject<Option<SignalCategory>>,
    pub data: BehaviorSubject<DataFrame>,
    pub current_trade_arc: Arc<Mutex<Option<Trade>>>,
    pub orders_listener: BehaviorSubject<Vec<Order>>,
    pub temp_executions: Arc<Mutex<Vec<Execution>>>,
    pub executions_listener: BehaviorSubject<Vec<Execution>>,
    pub leverage_subject: BehaviorSubject<Leverage>,
}

impl Trader {
    pub fn new(
        symbols: &[String; 2],
        initial_balance: f64,
        current_balance: BehaviorSubject<f64>,
        market_data_feed: MarketDataFeed,
        strategy_arc: Arc<Mutex<Strategy>>,
        performance_arc: Arc<Mutex<Performance>>,
        signal_generator: BehaviorSubject<Option<SignalCategory>>,
        data: BehaviorSubject<DataFrame>,
        orders_listener: BehaviorSubject<Vec<Order>>,
        executions_listener: BehaviorSubject<Vec<Execution>>,
        leverage_subject: BehaviorSubject<Leverage>,
        log_level: &LogLevel,
    ) -> Trader {
        Trader {
            symbols: symbols.clone(),
            initial_balance,
            current_balance,
            units: 0,
            market_data_feed: market_data_feed,
            strategy_arc: strategy_arc.clone(),
            performance_arc: performance_arc.clone(),
            signal_generator,
            data,
            current_trade_arc: Arc::new(Mutex::new(None)),
            orders_listener,
            temp_executions: Arc::new(Mutex::new(Vec::new())),
            leverage_subject: leverage_subject.clone(),
            executions_listener,
        }
    }

    pub async fn init(mut self) {
        let market_data = self.market_data_feed.clone();

        let handle1 = tokio::spawn(async move {
            market_data.fetch_benchmark_data().await;
        });

        let handle2 = tokio::spawn(async move {
            self.market_data_feed.init_binance_ws().await;
        });

        let handle3 = tokio::spawn(async move {
            let mut subscription = self.signal_generator.subscribe();
            while let Some(signal_opt) = subscription.next().await {
                let mut df = self.data.value();
                println!("@@@ SIGNAL OPT {:?}", signal_opt);
                df = process_last_signal(signal_opt, df).await.unwrap();
            }
        });

        let current_trade_arc = self.current_trade_arc.clone();
        let temp_executions = self.temp_executions.clone();

        let handle4 = tokio::spawn(async move {
            let mut subscription = self.executions_listener.subscribe();
            while let Some(latest_order_executions) = subscription.next().await {
                // get current trade state
                let mut current_trade_guard = current_trade_arc
                    .lock()
                    .expect("handle4 -> current_trade_guard deadlock");
                // match it
                match &mut *current_trade_guard {
                    // if there is a trade
                    Some(current_trade) => {
                        // get current trade open order id
                        let open_order_id = current_trade.get_open_order_id();

                        // iterate through latest executions
                        latest_order_executions
                            .clone()
                            .into_iter()
                            .for_each(|execution| {
                                //checks if execution is related to open order
                                if execution.order_id == open_order_id {
                                    current_trade.open_order.executions.push(execution);
                                    return;
                                }
                                // if execution is not related to open order, check if it is related to close order
                                if let Some(close_order) = &mut current_trade.close_order {
                                    // by checking if close_order's id is equal to execution's order_id
                                    if close_order.id == execution.order_id {
                                        close_order.executions.push(execution);
                                        return;
                                    }
                                }
                                // if none of the above apply
                                let mut temp_executions_guard = temp_executions.lock().expect(
                                    "handle4 -> Some arm -> temp_executions_guard deadlock",
                                );
                                // push each new execution into temp vector
                                temp_executions_guard.push(execution);
                            });
                    }
                    // if not
                    None => {
                        // iterate through latest executions
                        latest_order_executions
                            .clone()
                            .into_iter()
                            .for_each(|execution| {
                                // get temp_executions state
                                let mut temp_executions_guard = temp_executions.lock().expect(
                                    "handle4 -> None arm -> temp_executions_guard deadlock",
                                );
                                // and push each new execution into temp vector
                                temp_executions_guard.push(execution);
                            });
                    }
                }
            }
        });

        let current_trade_arc = self.current_trade_arc.clone();
        let temp_executions = self.temp_executions.clone();

        let handle5 = tokio::spawn(async move {
            let mut subscription = self.orders_listener.subscribe();
            while let Some(latest_orders) = subscription.next().await {
                // 1.update orders
                // iterate though from emitted orders
                // TODO: REMOVE FROM VEC
                latest_orders
                    .clone()
                    .into_iter()
                    // filter trigger orders
                    .filter(|latest_order| !latest_order.is_trigger_order())
                    .for_each(|latest_order| {
                        // get current trade state
                        let mut current_trade_guard = current_trade_arc
                            .lock()
                            .expect("handle5 -> current_trade_guard deadlock");

                        let mut temp_executions_guard = temp_executions
                            .lock()
                            .expect("handle5 -> None arm -> temp_executions_guard deadlock");
                        // match it
                        match &mut *current_trade_guard {
                            Some(current_trade) => {
                                // updates order
                            }
                            None => {
                                let previous_executions = temp_executions_guard
                                    .iter()
                                    .cloned()
                                    .enumerate()
                                    .filter(|(index, execution)| execution.id == latest_order.id)
                                    .collect::<Vec<(usize, Execution)>>();

                                let orders_executions = previous_executions.into_iter().fold(
                                    Vec::new(),
                                    |mut acc: Vec<_>, (index, execution)| {
                                        let removed_execution = temp_executions_guard.remove(index);
                                        acc.push(removed_execution);
                                        acc
                                    },
                                );

                                let open_order = Order::new(
                                    latest_order.id,
                                    latest_order.order_type,
                                    latest_order.symbol.clone(),
                                    latest_order.status,
                                    latest_order.position,
                                    orders_executions,
                                    latest_order.stop_loss_price,
                                    latest_order.take_profit_price,
                                    false,
                                    None,
                                );
                                let leverage = self.leverage_subject.value();

                                *current_trade_guard = Some(Trade::new(
                                    latest_order.symbol,
                                    leverage.get_factor(),
                                    open_order,
                                ));
                            }
                        }
                        // check trade status
                    });
                // 2.update trade
                // 3.check trade closing
            }
        });

        let _ = handle1.await;
        let _ = handle2.await;
        let _ = handle3.await;
        let _ = handle4.await;
        let _ = handle5.await;
    }
}

async fn process_last_signal(
    signal_opt: Option<SignalCategory>,
    df: DataFrame,
) -> Result<DataFrame, Error> {
    match signal_opt {
        Some(signal) => match signal {
            SignalCategory::CloseLong
            | SignalCategory::CloseShort
            | SignalCategory::ClosePosition => Ok(df),
            SignalCategory::GoLong | SignalCategory::GoShort => Ok(df),
            _ => Ok(df),
        },

        None => Ok(df),
    }
}

#[derive(Clone)]
pub struct Trade {
    pub symbol: String,
    // pub status: TradeStatus,
    pub leverage: f64,
    pub open_order: Order,
    pub close_order: Option<Order>,
}

impl Trade {
    fn new(symbol: String, leverage: f64, open_order: Order) -> Self {
        Trade {
            symbol,
            leverage,
            open_order,
            close_order: None,
        }
    }

    fn get_open_order_id(&self) -> String {
        self.open_order.id
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum TradeStatus {
    New,
    Open,
    Closed,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: String,
    pub symbol: String,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub position: i32,
    pub executions: Vec<Execution>,
    pub stop_loss_price: Option<f64>,
    pub take_profit_price: Option<f64>,
    pub is_cancel_order: bool,
    pub stop_order_type: Option<StopOrderType>,
}

impl Order {
    fn new(
        id: String,
        order_type: OrderType,
        symbol: String,
        status: OrderStatus,
        position: i32,
        executions: Vec<Execution>,
        stop_loss_price: Option<f64>,
        take_profit_price: Option<f64>,
        is_cancel_order: bool,
        stop_order_type: Option<StopOrderType>,
    ) -> Self {
        Order {
            id,
            order_type,
            symbol,
            status,
            position,
            executions,
            stop_loss_price,
            take_profit_price,
            is_cancel_order,
            stop_order_type,
        }
    }

    fn update(
        &mut self,
        status: OrderStatus,
        stop_loss_price: Option<f64>,
        take_profit_price: Option<f64>,
        executions: Vec<Execution>,
    ) {
        self.status = status;
        self.stop_loss_price = stop_loss_price;
        self.take_profit_price = take_profit_price;
        self.executions.extend(executions);
    }

    fn is_trigger_order(&self) -> bool {
        self.stop_order_type.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    StandBy,
    PartiallyFilled,
    Filled,
    Canceled,
}

#[derive(Debug, Clone)]
pub struct Execution {
    id: String,
    order_id: String,
    order_type: OrderType,
    timestamp: i64,
    price: f64,
    qty: f64,
    fee: f64,
    fee_rate: f64,
    is_maker: bool,
    closed_qty: f64,
}

impl Execution {
    fn get_value(&self) -> f64 {
        self.qty * self.price
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, Deserialize)]
pub enum StopOrderType {
    StopLoss,
    TakeGain,
}
