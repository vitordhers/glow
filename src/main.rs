// use tokio::*;
extern crate dotenv;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dotenv::dotenv;
pub mod shared;
mod trader;
use polars::prelude::DataFrame;
use trader::{
    enums::{
        balance::Balance,
        log_level::LogLevel,
        modifiers::{leverage::Leverage, position_lock::PositionLock, price_level::PriceLevel},
        order_action::OrderAction,
        order_type::OrderType,
        trading_data_update::TradingDataUpdate,
    },
    exchanges::bybit::BybitExchange,
    functions::current_datetime,
    indicators::{
        ema_trend::ExponentialMovingAverageTrendIndicator, stc::STCIndicator,
        stochastic::StochasticIndicator, stochastic_threshold::StochasticThresholdIndicator,
        tsi::TSIIndicator, IndicatorWrapper,
    },
    models::{
        behavior_subject::BehaviorSubject, execution::Execution, trade::Trade,
        trading_settings::TradingSettings,
    },
    modules::{data_feed::DataFeed, performance::Performance, strategy::Strategy, trader::Trader},
    signals::{
        tsi_stc::{
            TSISTCCloseLongSignal, TSISTCCloseShortSignal, TSISTCLongSignal, TSISTCShortSignal,
        },
        SignalWrapper,
    },
    traits::exchange::Exchange,
};

use std::marker::Send;

// mod optimization;

use crate::trader::{enums::modifiers::price_level::TrailingStopLoss, models::contract::Contract};

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();
    // let trend_col = String::from("bullish_market");
    use std::env::{self};
    let max_rows = "40".to_string();
    env::set_var("POLARS_FMT_MAX_ROWS", max_rows);

    // ARGS
    let windows = vec![5]; // 5, 15, 30 // 5, 9, 12
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 0;

    let btcusdt_contract: Contract = Contract::new(
        String::from("BTCUSDT"),
        0.0001,
        Duration::hours(8),
        None,
        0.00005,
        100.0,
        0.001,
        100.0,
    );

    let agixusdt_contract: Contract = Contract::new(
        String::from("AGIXUSDT"),
        0.000073,
        Duration::hours(8),
        None,
        0.00005,
        300000.0,
        1.0,
        25.0,
    );

    let arbusdt_contract: Contract = Contract::new(
        String::from("ARBUSDT"),
        0.0001,
        Duration::hours(8),
        None,
        0.0001,
        180000.0,
        0.1,
        50.0,
    );

    let ethusdt_contract: Contract = Contract::new(
        String::from("ETHUSDT"),
        0.0001,
        Duration::hours(8),
        None,
        0.01,
        1500.0,
        0.01,
        50.0,
    );

    let linkusdt_contract: Contract = Contract::new(
        String::from("LINKUSDT"),
        0.000047,
        Duration::hours(8),
        None,
        0.001,
        36750.0,
        0.1,
        50.0,
    );

    let mut contracts = HashMap::new();
    contracts.insert(btcusdt_contract.symbol.clone(), btcusdt_contract);
    contracts.insert(agixusdt_contract.symbol.clone(), agixusdt_contract);
    contracts.insert(arbusdt_contract.symbol.clone(), arbusdt_contract);
    contracts.insert(linkusdt_contract.symbol.clone(), linkusdt_contract);
    contracts.insert(ethusdt_contract.symbol.clone(), ethusdt_contract);

    let start_datetime = current_datetime();

    let start_date = NaiveDate::from_ymd_opt(2023, 9, 28).unwrap();
    let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

    let start_datetime = NaiveDateTime::new(start_date, start_time);

    let seconds_to_next_full_minute = 60 - start_datetime.timestamp() % 60;
    let trading_initial_datetime = start_datetime + Duration::seconds(seconds_to_next_full_minute);

    // let stochastic_indicator = StochasticIndicator {
    //     name: "StochasticIndicator".into(),
    //     windows: windows.clone(),
    //     anchor_symbol: anchor_symbol.clone(),
    //     starting_datetime: trading_initial_datetime,
    //     k_window: 14,
    //     k_smooth: 4,
    //     d_smooth: 5,
    // };

    let anchor_symbol = String::from("BTCUSDT");
    let traded_symbol = String::from("AGIXUSDT");

    let stc_indicator = STCIndicator {
        name: "STC indicator".to_string(),
        anchor_symbol: anchor_symbol.clone(),
        windows: windows.clone(),
        length: 12,
        short_span: 24,
        long_span: 50,
        weight: 0.5,
    };

    let tsi_indicator = TSIIndicator {
        name: "True Strength Index Indicator".to_string(),
        anchor_symbol: anchor_symbol.clone(),
        windows: windows.clone(),
        long_span: 14,
        short_span: 1,
    };

    // let stochastic_threshold_indicator = StochasticThresholdIndicator {
    //     name: "StochasticThresholdIndicator".into(),
    //     upper_threshold,
    //     lower_threshold,
    //     trend_col: trend_col.clone(),
    // };

    // let ewm_preindicator = ExponentialMovingAverageTrendIndicator {
    //     name: "ExponentialMovingAverageTrendIndicator".into(),
    //     anchor_symbol: anchor_symbol.clone(),
    //     long_span: 50,
    //     short_span: 21,
    //     trend_col: trend_col.clone(),
    // };

    let pre_indicators: Vec<IndicatorWrapper> = vec![
    // ewm_preindicator.into()
    ];

    let indicators: Vec<IndicatorWrapper> = vec![
        stc_indicator.into(),
        tsi_indicator.into(), // stochastic_threshold_indicator.into(),
                              // stochastic_indicator.into(),
    ];

    let signals_period = 1;

    let short_signal = TSISTCShortSignal {
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
        period: signals_period,
    };

    let long_signal = TSISTCLongSignal {
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
        period: signals_period,
    };

    let close_short_signal = TSISTCCloseShortSignal {
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
        period: signals_period,
    };

    let close_long_signal = TSISTCCloseLongSignal {
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
        period: signals_period,
    };

    let signals: Vec<SignalWrapper> = vec![
        short_signal.into(),
        long_signal.into(),
        close_short_signal.into(),
        close_long_signal.into(),
    ];

    let bybit_exchange = BybitExchange::new(
        "Bybit".to_string(),
        contracts,
        anchor_symbol.clone(),
        traded_symbol.clone(),
        0.0002,
        0.00055,
    );

    let exchange = Box::new(bybit_exchange);

    let exchange_listener: BehaviorSubject<Box<dyn Exchange + Send + Sync>> =
        BehaviorSubject::new(exchange);

    let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
    let timestamp = Utc::now().timestamp() * 1000;
    let current_balance_listener = BehaviorSubject::new(Balance::new(timestamp, 0.0, 0.0));

    let bar_length = 60;
    let log_level = LogLevel::All;
    let benchmark_balance = 100.00;

    let performance = Performance::new(exchange_listener.clone(), trading_initial_datetime);
    let performance_arc = Arc::new(Mutex::new(performance));

    let signal_listener = BehaviorSubject::new(None);
    let trading_data_listener = BehaviorSubject::new(DataFrame::default());

    let initial_leverage = Leverage::Isolated(20);

    let position_lock_modifier = PositionLock::Nil;

    let mut price_level_modifiers = HashMap::new();
    let stop_loss: PriceLevel = PriceLevel::StopLoss(0.4);
    let take_profit = PriceLevel::TakeProfit(1.0);

    let trailing_stop_loss = TrailingStopLoss::Percent(0.5, 0.1);
    let trailing_modifier = PriceLevel::TrailingStopLoss(trailing_stop_loss);

    price_level_modifiers.insert(stop_loss.get_hash_key(), stop_loss);
    price_level_modifiers.insert(take_profit.get_hash_key(), take_profit);
    // price_level_modifiers.insert("tsl".to_string(), trailing_modifier);

    let trading_settings_arc = Arc::new(Mutex::new(TradingSettings::new(
        OrderType::Market,
        OrderType::Market,
        initial_leverage.clone(),
        position_lock_modifier,
        price_level_modifiers,
        0.95, // CHANGE ALLOCATION
    )));

    let strategy = Strategy::new(
        "Stepped Stochastic Strategy".into(),
        pre_indicators,
        indicators.clone(),
        signals.clone(),
        benchmark_balance,
        performance_arc.clone(),
        trading_settings_arc.clone(),
        exchange_listener.clone(),
        current_trade_listener.clone(),
        signal_listener.clone(),
        current_balance_listener.clone(),
    );

    let strategy_arc = Arc::new(Mutex::new(strategy));
    let exchange_socket_error_arc = Arc::new(Mutex::new(None));

    let trading_data_update_listener = BehaviorSubject::new(TradingDataUpdate::Nil);
    let update_balance_listener: BehaviorSubject<Option<Balance>> = BehaviorSubject::new(None);
    let update_order_listener: BehaviorSubject<Option<OrderAction>> = BehaviorSubject::new(None);
    let update_executions_listener: BehaviorSubject<Vec<Execution>> = BehaviorSubject::new(vec![]);

    let data_feed = DataFeed::new(
        bar_length,
        log_level,
        &exchange_socket_error_arc,
        &trading_data_update_listener,
        &exchange_listener,
        &trading_data_listener,
        &update_balance_listener,
        &update_order_listener,
        &update_executions_listener,
        &current_trade_listener,
        start_datetime,
        indicators,
        signals,
    );

    let leverage_listener = BehaviorSubject::new(initial_leverage);
    let trader = Trader::new(
        data_feed,
        &strategy_arc,
        &performance_arc,
        &trading_settings_arc,
        &exchange_socket_error_arc,
        &exchange_listener,
        &current_balance_listener,
        &update_balance_listener,
        &update_order_listener,
        &update_executions_listener,
        &signal_listener,
        &trading_data_listener,
        &trading_data_update_listener,
        &current_trade_listener,
        &leverage_listener,
        &log_level,
        true,
    );

    let _ = trader.init().await;
}
