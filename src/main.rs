// use tokio::*;
extern crate dotenv;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::{Duration, Utc};
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
    indicators::{
        ExponentialMovingAverageIndicator, IndicatorWrapper, StochasticIndicator,
        StochasticThresholdIndicator,
    },
    models::{
        behavior_subject::BehaviorSubject, execution::Execution, trade::Trade,
        trading_settings::TradingSettings,
    },
    modules::{data_feed::DataFeed, performance::Performance, strategy::Strategy, trader::Trader},
    signals::{
        MultipleStochasticWithThresholdCloseLongSignal,
        MultipleStochasticWithThresholdCloseShortSignal, MultipleStochasticWithThresholdLongSignal,
        MultipleStochasticWithThresholdShortSignal, SignalWrapper,
    },
    traits::exchange::Exchange,
};

use std::marker::Send;

// mod optimization;

use crate::trader::models::contract::Contract;

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();
    let anchor_symbol = String::from("BTCUSDT");
    let trend_col = String::from("bullish_market");

    // ARGS
    let windows = vec![3, 5, 15]; // 5, 15, 30 // 5, 9, 12
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 2;

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

    let linlusdt_contract: Contract = Contract::new(
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
    contracts.insert(linlusdt_contract.symbol.clone(), linlusdt_contract);

    let stochastic_indicator = StochasticIndicator {
        name: "StochasticIndicator".into(),
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
    };

    let stochastic_threshold_indicator = StochasticThresholdIndicator {
        name: "StochasticThresholdIndicator".into(),
        upper_threshold,
        lower_threshold,
        trend_col: trend_col.clone(),
    };

    let ewm_preindicator = ExponentialMovingAverageIndicator {
        name: "ExponentialMovingAverageIndicator".into(),
        anchor_symbol: anchor_symbol.clone(),
        long_span: 50,
        short_span: 21,
        trend_col: trend_col.clone(),
    };

    let pre_indicators: Vec<IndicatorWrapper> = vec![ewm_preindicator.into()];

    let indicators: Vec<IndicatorWrapper> = vec![
        stochastic_indicator.into(),
        stochastic_threshold_indicator.into(),
    ];

    let multiple_stochastic_with_threshold_short_signal =
        MultipleStochasticWithThresholdShortSignal {
            windows: windows.clone(),
            anchor_symbol: anchor_symbol.clone(),
        };

    let multiple_stochastic_with_threshold_long_signal =
        MultipleStochasticWithThresholdLongSignal {
            windows: windows.clone(),
            anchor_symbol: anchor_symbol.clone(),
        };

    let multiple_stochastic_with_threshold_close_short_signal =
        MultipleStochasticWithThresholdCloseShortSignal {
            anchor_symbol: anchor_symbol.clone(),
            windows: windows.clone(),
            upper_threshold,
            lower_threshold,
            close_window_index,
        };

    let multiple_stochastic_with_threshold_close_long_signal =
        MultipleStochasticWithThresholdCloseLongSignal {
            anchor_symbol: anchor_symbol.clone(),
            windows: windows.clone(),
            upper_threshold,
            lower_threshold,
            close_window_index,
        };

    let signals: Vec<SignalWrapper> = vec![
        multiple_stochastic_with_threshold_short_signal.into(),
        multiple_stochastic_with_threshold_long_signal.into(),
        multiple_stochastic_with_threshold_close_short_signal.into(),
        multiple_stochastic_with_threshold_close_long_signal.into(),
    ];

    let bybit_exchange = BybitExchange::new(
        "Bybit".to_string(),
        contracts,
        "BTCUSDT".to_string(),
        "AGIXUSDT".to_string(),
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
    let initial_data_offset = 225; // TODO: create a way to calculate this automatically
    let benchmark_balance = 100.00;

    let performance = Performance::new(exchange_listener.clone());
    let performance_arc = Arc::new(Mutex::new(performance));

    let signal_listener = BehaviorSubject::new(None);
    let trading_data_listener = BehaviorSubject::new(DataFrame::default());
    let initial_leverage = Leverage::Isolated(25);

    let position_lock_modifier = PositionLock::Fee;

    let mut price_level_modifiers = HashMap::new();
    let stop_loss: PriceLevel = PriceLevel::StopLoss(0.40);
    let take_profit = PriceLevel::TakeProfit(1.0);
    price_level_modifiers.insert(stop_loss.get_hash_key(), stop_loss);
    price_level_modifiers.insert(take_profit.get_hash_key(), take_profit);

    let trading_settings_arc = Arc::new(Mutex::new(TradingSettings::new(
        OrderType::Limit,
        OrderType::Market,
        initial_leverage.clone(),
        position_lock_modifier,
        price_level_modifiers,
        0.95, // CHANGE ALLOCATION
    )));

    let strategy = Strategy::new(
        "Stepped Stochastic Strategy".into(),
        pre_indicators,
        indicators,
        signals,
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
        initial_data_offset,
        log_level,
        &exchange_socket_error_arc,
        &trading_data_update_listener,
        &exchange_listener,
        &trading_data_listener,
        &update_balance_listener,
        &update_order_listener,
        &update_executions_listener,
        &current_trade_listener,
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
    );

    let _ = trader.init().await;
}
