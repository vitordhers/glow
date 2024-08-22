use chrono::{Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use cli::{change_benchmark_datetimes, select_from_list};
use common::{
    enums::{
        balance::Balance,
        log_level::LogLevel,
        modifiers::{leverage::Leverage, position_lock::PositionLock, price_level::PriceLevel},
        order_action::OrderAction,
        order_type::OrderType,
        trading_data_update::TradingDataUpdate,
    },
    functions::current_datetime,
    structs::{BehaviorSubject, Contract, Execution, SymbolsPair, Trade, TradingSettings},
};
use core::data_feed::DataFeed;
use dotenv::dotenv;
use exchanges::enums::{
    DataProviderExchangeId, DataProviderExchangeWrapper, TraderExchangeId, TraderExchangeWrapper,
};
use futures_util::StreamExt;
use polars::prelude::DataFrame;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use std::{env, io::stdin, marker::Send};
use strategy::{Strategy, StrategyId};
use tokio::{join, spawn};

#[tokio::main]
async fn main() {
    dotenv().ok();
    let is_test_mode = true;
    let max_rows = "40".to_string();
    env::set_var("POLARS_FMT_MAX_ROWS", max_rows);

    println!("Glow Backtesting Suite - v0.02.");
    println!(
        r#"Gloria Patri, et Filio, et Spiritui Sancto.
        Sicut erat in principio, et nunc et semper, et in saecula saeculorum.
        Amen 🙏"#
    );

    // inputs:
    // * symbols *
    // depends on: []
    // updates: [trading_settings, strategy]
    // * trading settings *
    // depends on: [symbols]

    let selected_symbols_pair = BehaviorSubject::new(SymbolsPair::default());
    let default_trading_settings = TradingSettings::load_or_default();
    let current_trading_settings = BehaviorSubject::new(TradingSettings::load_or_default());

    let ctsc = current_trading_settings.clone();
    let selected_symbols_handle = spawn(async move {
        let mut selection_subscription = selected_symbols_pair.subscribe();
        // let current_trading_settings = current_trading_settings.clone();

        while let Some(updated_symbols_pair) = selection_subscription.next().await {
            let updated_trading_settings = ctsc.value().patch_symbols_pair(updated_symbols_pair);
            ctsc.next(updated_trading_settings);
        }
    });

    selected_symbols_handle.await;

    let trading_settings: Arc<RwLock<TradingSettings>> =
        Arc::new(RwLock::new(default_trading_settings));

    let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
    let trader_last_ws_error_ts: Arc<Mutex<Option<i64>>> = Arc::new(Mutex::new(None));

    let update_balance_listener: BehaviorSubject<Option<Balance>> = BehaviorSubject::new(None);
    let update_executions_listener: BehaviorSubject<Vec<Execution>> = BehaviorSubject::new(vec![]);
    let update_order_listener: BehaviorSubject<Option<OrderAction>> = BehaviorSubject::new(None);

    let default_trader_exchange_id = TraderExchangeId::default();
    let selected_trader_exchange_id = BehaviorSubject::new(default_trader_exchange_id);
    let default_trader_exchange = TraderExchangeWrapper::new(
        default_trader_exchange_id,
        &current_trade_listener,
        &trader_last_ws_error_ts,
        &trading_settings,
        &update_balance_listener,
        &update_executions_listener,
        &update_order_listener,
    );
    let current_trader_exchange = BehaviorSubject::new(default_trader_exchange);
    let ctsc = current_trading_settings.clone();
    let cte = current_trader_exchange.clone();
    let selected_trader_exchange_handle = spawn(async move {
        let selection_subscription = &mut selected_trader_exchange_id.subscribe();
        // let mut settings_subscription = ctsc.subscribe();

        while let Some(selected_exchange_id) = selection_subscription.next().await {
            let updated_trader_exchange = TraderExchangeWrapper::new(
                selected_exchange_id,
                &current_trade_listener,
                &trader_last_ws_error_ts,
                &trading_settings,
                &update_balance_listener,
                &update_executions_listener,
                &update_order_listener,
            );
            &current_trader_exchange.next(updated_trader_exchange);
        }
    });

    selected_trader_exchange_handle.await;

    let selected_strategy_id = BehaviorSubject::new(StrategyId::default());
    let current_strategy = BehaviorSubject::new(get_default_strategy());
    let csc = current_strategy.clone();
    let selected_strategy_handle = spawn(async move {
        let selection_subscription = &mut selected_strategy_id.subscribe();

        while let Some(selected_strategy_id) = selection_subscription.next().await {
            let updated_strategy = Strategy::new(name, preindicators, indicators, signals);
        }
    });

    selected_strategy_handle.await;

    let data_provider_last_ws_error_ts: Arc<Mutex<Option<i64>>> = Arc::new(Mutex::new(None));
    let trading_data_update_listener = BehaviorSubject::new(TradingDataUpdate::default());
    let selected_data_provider_exchange = BehaviorSubject::new(DataProviderExchangeId::default());

    let current_data_provider_exchange = Arc::new(Mutex::new(None));
    // TODO: validate this comment below:
    // note that only who provides subscription must be cloned
    let ctsc = current_trading_settings.clone();
    let csc = current_strategy.clone();
    let tudl = trading_data_update_listener.clone();

    let selected_data_provider_handle = spawn(async move {
        let mut selection_subscription = selected_data_provider_exchange.subscribe();
        let mut settings_subscription = ctsc.subscribe();
        let mut strategy_subscription = csc.subscribe();

        while let (
            Some(selected_exchange),
            Some(TradingSettings {
                granularity,
                symbols_pair,
                ..
            }),
            Some(strategy),
        ) = join!(
            selection_subscription.next(),
            settings_subscription.next(),
            strategy_subscription.next()
        ) {
            let updated_data_provider_exchange = DataProviderExchangeWrapper::new(
                selected_exchange,
                granularity.get_duration(),
                &data_provider_last_ws_error_ts,
                strategy.get_minimum_klines_for_benchmarking(),
                symbols_pair,
                &tudl,
            );
            let mut guard = current_data_provider_exchange.lock().unwrap();
            *guard = Some(updated_data_provider_exchange);
        }
    });

    selected_data_provider_handle.await;

    // let mut current_strategy = get_default_strategy();
    let mut end_datetime = current_datetime();
    let mut start_datetime: NaiveDateTime;

    loop {
        let options = vec![
            "Select Benchmark Datetimes",
            "Change Trading Coins",
            "Select Data Provider Exchange",
            "Select Trader Exchange",
            "Change Strategy",
            "Change Trading Settings",
            "Run Benchmark",
        ];

        let default_index = options.len() + 1;
        let selection = select_from_list("", &options, Some(default_index));
        match selection {
            0 => {
                let result = change_benchmark_datetimes(
                    start_datetime,
                    end_datetime,
                    &current_trader_exchange.value(),
                    &current_strategy.value(),
                );
                if result.is_some() {
                    let (updated_start_datetime, updated_end_datetime) = result.unwrap();
                    start_datetime = updated_start_datetime;
                    end_datetime = updated_end_datetime;
                }
                continue;
            }
            1 => {
                // CHANGE SYMBOLS PAIR
            }
            2 => {
                // CHANGE PROVIDER EXCHANGE
            }
            3 => {
                // CHANGE TRADER EXCHANGE
            }
            4 => {
                // CHANGE STRATEGY
            }
            5 => {
                // CHANGE TRADING SETTINGS
            }
            6 => {
                // RUN BENCHMARK
            }
            _ => {
                println!("Invalid option");
                continue;
            }
        }
    }

    // ARGS
    // let windows = vec![1]; // 5, 15, 30 // 5, 9, 12

    // let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    // start_datetime = NaiveDateTime::new(start_date, start_time);

    // let seconds_to_next_full_minute = 60 - start_datetime.timestamp() % 60;
    // let trading_initial_datetime = start_datetime + Duration::seconds(seconds_to_next_full_minute);

    // let cloud_length = 20;

    // let signals: Vec<SignalWrapper> = vec![
    //     multiple_stochastic_with_threshold_short_signal.into(),
    //     multiple_stochastic_with_threshold_long_signal.into(),
    //     multiple_stochastic_with_threshold_close_short_signal.into(),
    //     multiple_stochastic_with_threshold_close_long_signal.into(),
    // ];

    // let initial_leverage = Leverage::Isolated(5);
    // let position_lock_modifier = PositionLock::None;
    // let mut price_level_modifiers = HashMap::new();
    // let stop_loss = PriceLevel::StopLoss(0.25);
    // let take_profit = PriceLevel::TakeProfit(0.5);
    // // let trailing_stop_loss = TrailingStopLoss::Percent(0.5, 0.05);
    // // let trailing_modifier = PriceLevel::TrailingStopLoss(trailing_stop_loss);
    // price_level_modifiers.insert(stop_loss.get_hash_key(), stop_loss);
    // price_level_modifiers.insert(take_profit.get_hash_key(), take_profit);
    // // price_level_modifiers.insert("tsl".to_string(), trailing_modifier);

    // let trading_settings_arc = Arc::new(Mutex::new(TradingSettings::new(
    //     OrderType::Market,
    //     OrderType::Market,
    //     initial_leverage.clone(),
    //     position_lock_modifier,
    //     price_level_modifiers,
    //     0.95, // CHANGE ALLOCATION,
    //     false,
    // )));

    // let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
    // let data_provider_exchange_socket_error_ts = Arc::new(Mutex::new(None));
    // let data_provider_exchange_wrapper = DataProviderExchangeWrapper::new_binance_data_provider(
    //     benchmark_end,
    //     kline_data_schema,
    //     kline_duration,
    //     last_ws_error_ts,
    //     minimum_klines_for_benchmarking,
    //     symbols,
    //     trading_data_schema,
    //     trading_data_update_listener,
    // );

    // let data_feed = DataFeed::new(
    //     &current_trade_listener,
    //     data_provider_exchange_socket_error_ts,
    //     log_level,
    //     &exchange_socket_error_arc,
    //     &trading_data_update_listener,
    //     &exchange_listener,
    //     &trading_data_listener,
    //     &update_balance_listener,
    //     &update_order_listener,
    //     &update_executions_listener,
    //     &current_trade_listener,
    //     start_datetime,
    //     indicators,
    //     signals,
    //     is_test_mode,
    // );

    // let exchange = Box::new(bybit_exchange);

    // let exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>> =
    //     BehaviorSubject::new(exchange);

    // let timestamp = Utc::now().timestamp() * 1000;
    // let current_balance_listener = BehaviorSubject::new(Balance::new(timestamp, 0.0, 0.0));

    // let bar_length = 60;
    // let log_level = LogLevel::All;
    // let benchmark_balance = 100.00;

    // let performance = Performance::new(exchange_listener.clone(), trading_initial_datetime);
    // let performance_arc = Arc::new(Mutex::new(performance));

    // let signal_listener = BehaviorSubject::new(None);
    // let trading_data_listener = BehaviorSubject::new(DataFrame::default());

    // let strategy = Strategy::new(
    //     "Stepped Stochastic Strategy".to_string(),
    //     pre_indicators,
    //     indicators.clone(),
    //     signals.clone(),
    //     benchmark_balance,
    //     performance_arc.clone(),
    //     trading_settings_arc.clone(),
    //     exchange_listener.clone(),
    //     current_trade_listener.clone(),
    //     signal_listener.clone(),
    //     current_balance_listener.clone(),
    // );

    // let strategy_arc = Arc::new(Mutex::new(strategy));

    // let trading_data_update_listener = BehaviorSubject::new(TradingDataUpdate::None);
    // let update_balance_listener: BehaviorSubject<Option<Balance>> = BehaviorSubject::new(None);
    // let update_order_listener: BehaviorSubject<Option<OrderAction>> = BehaviorSubject::new(None);
    // let update_executions_listener: BehaviorSubject<Vec<Execution>> = BehaviorSubject::new(vec![]);

    // let leverage_listener = BehaviorSubject::new(initial_leverage);
    // let trader = Trader::new(
    //     data_feed,
    //     &strategy_arc,
    //     &performance_arc,
    //     &trading_settings_arc,
    //     &exchange_socket_error_arc,
    //     &exchange_listener,
    //     &current_balance_listener,
    //     &update_balance_listener,
    //     &update_order_listener,
    //     &update_executions_listener,
    //     &signal_listener,
    //     &trading_data_listener,
    //     &trading_data_update_listener,
    //     &current_trade_listener,
    //     &leverage_listener,
    //     &log_level,
    //     is_test_mode,
    // );

    // let _ = trader.init().await;
}
