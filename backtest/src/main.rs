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
    }, functions::current_datetime, structs::{BehaviorSubject, Contract, Execution, Trade, TradingSettings}
};
use exchanges::enums::DataProviderExchangeWrapper;
use modules::data_feed::DataFeed;
use polars::prelude::DataFrame;
use strategy::Strategy;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use std::{env, io::stdin, marker::Send};

#[tokio::main]
async fn main() {
    let is_test_mode = true;
    let max_rows = "40".to_string();
    env::set_var("POLARS_FMT_MAX_ROWS", max_rows);

    println!("Glow Backtesting Suite - v0.02.");
    println!(
        r#"Gloria Patri, et Filio, et Spiritui Sancto.
        Sicut erat in principio, et nunc et semper, et in saecula saeculorum.
        Amen 🙏"#
    );

    let mut trading_settings = TradingSettings::load_or_default();
    let mut strategy = Strategy::

    let mut end_datetime = current_datetime();
    let mut start_datetime;

    loop {
        let options = vec![
            "Select Benchmark Datetimes",
            "Select Data Provider Exchange",
            "Select Trader Exchange",
            "Change Trading Coins",
            "Change Strategy",
            "Change Trading Settings",
            "Run Benchmark",
        ];
        let default_index = options.len() + 1;
        let selection = select_from_list("", &options, Some(default_index));
        match selection {
            0 => change_benchmark_datetimes(),
        }
    }

    // ARGS
    // let windows = vec![1]; // 5, 15, 30 // 5, 9, 12

    // let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    // start_datetime = NaiveDateTime::new(start_date, start_time);

    // let seconds_to_next_full_minute = 60 - start_datetime.timestamp() % 60;
    // let trading_initial_datetime = start_datetime + Duration::seconds(seconds_to_next_full_minute);

    // let cloud_length = 20;

    let fast_slow_mas_indicator = FastSlowRelativeMovingAverages {
        name: "FastSlowRelativeMovingAverages".to_string(),
        contract: traded_contract,
        fast_period: 20,
        slow_period: 200,
        cloud_length,
        momentum_diff_span: 15,
    };

    let pre_indicators: Vec<IndicatorWrapper> = vec![];

    let indicators: Vec<IndicatorWrapper> = vec![fast_slow_mas_indicator.into()];

    let multiple_stochastic_with_threshold_short_signal =
        MultipleStochasticWithThresholdShortSignal {
            windows: windows.clone(),
            anchor_symbol: anchor_symbol.clone(),
            cloud_length,
        };

    let multiple_stochastic_with_threshold_long_signal =
        MultipleStochasticWithThresholdLongSignal {
            windows: windows.clone(),
            anchor_symbol: anchor_symbol.clone(),
            cloud_length,
        };

    let multiple_stochastic_with_threshold_close_short_signal =
        MultipleStochasticWithThresholdCloseShortSignal {
            anchor_symbol: anchor_symbol.clone(),
            windows: windows.clone(),
            cloud_length,
        };

    let multiple_stochastic_with_threshold_close_long_signal =
        MultipleStochasticWithThresholdCloseLongSignal {
            anchor_symbol: anchor_symbol.clone(),
            windows: windows.clone(),
            cloud_length,
        };

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
