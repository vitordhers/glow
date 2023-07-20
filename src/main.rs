// use tokio::*;
extern crate dotenv;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::Duration;
use dotenv::dotenv;
pub mod shared;
mod trader;
use polars::prelude::DataFrame;
use trader::{
    enums::log_level::LogLevel,
    indicators::{
        ExponentialMovingAverageIndicator, StochasticIndicator, StochasticThresholdIndicator,
    },
    models::{
        behavior_subject::BehaviorSubject,
        exchange::Exchange,
        indicator::Indicator,
        market_data::MarketDataFeed,
        modifiers::{Leverage, PositionLockModifier, PriceLevelModifier},
        performance::Performance,
        signal::{SignalCategory, Signer},
        strategy::{self, Strategy},
    },
    signals::{
        MultipleStochasticWithThresholdCloseLongSignal,
        MultipleStochasticWithThresholdCloseShortSignal, MultipleStochasticWithThresholdLongSignal,
        MultipleStochasticWithThresholdShortSignal,
    },
    Trader,
};

use crate::trader::models::contract::Contract;

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();
    let anchor_symbol = String::from("BTCUSDT");
    let symbol = String::from("AGIXUSDT");
    let symbols = [anchor_symbol.clone(), symbol];

    let trend_col = String::from("bullish_market");

    let windows = vec![3, 5, 15]; // 5, 15, 30
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 2;

    let AGIXUSDT: Contract = Contract::new(
        String::from("AGIXUSDT"),
        0.000073,
        Duration::hours(8),
        None,
        0.00005,
        300000.0,
        1.0,
        25.0,
    );

    let ARBUSDT: Contract = Contract::new(
        String::from("ARBUSDT"),
        0.0001,
        Duration::hours(8),
        None,
        0.0001,
        180000.0,
        0.1,
        50.0,
    );

    let BTCUSDT: Contract = Contract::new(
        String::from("BTCUSDT"),
        0.0001,
        Duration::hours(8),
        None,
        0.00005,
        100.0,
        0.001,
        100.0,
    );

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

    let pre_indicators: Vec<Box<(dyn Indicator + std::marker::Send + Sync + 'static)>> =
        vec![Box::new(ewm_preindicator)];

    let indicators: Vec<Box<(dyn Indicator + std::marker::Send + Sync + 'static)>> = vec![
        Box::new(stochastic_indicator),
        Box::new(stochastic_threshold_indicator),
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

    let signals: Vec<Box<dyn Signer + std::marker::Send + Sync + 'static>> = vec![
        Box::new(multiple_stochastic_with_threshold_short_signal),
        Box::new(multiple_stochastic_with_threshold_long_signal),
        Box::new(multiple_stochastic_with_threshold_close_short_signal),
        Box::new(multiple_stochastic_with_threshold_close_long_signal),
    ];

    let exchange = Exchange::new(
        "Bybit".to_string(),
        0.0001,
        0.0006,
        "https://api.bybit.com/v5".to_string(),
        "wss://stream.bybit.com/v5/public/option".to_string(),
    );

    let lock_modifier = PositionLockModifier::Fee;

    let bar_length = 60;
    let log_level = LogLevel::All;
    let initial_data_offset = 225; // TODO: create a way to calculate this automatically
    let initial_balance = 100.00;
    let mut price_level_modifiers = HashMap::new();
    let stop_loss = PriceLevelModifier::StopLoss(0.44);
    let take_profit = PriceLevelModifier::TakeProfit(1.0);
    price_level_modifiers.insert(stop_loss.get_hash_key(), stop_loss);
    price_level_modifiers.insert(take_profit.get_hash_key(), take_profit);

    let performance = Performance::default();
    let performance_arc = Arc::new(Mutex::new(performance));

    let data_subject = BehaviorSubject::new(DataFrame::default());

    let strategy = Strategy::new(
        "Stepped Stochastic Strategy".into(),
        pre_indicators,
        indicators,
        signals,
        data_subject,
        initial_balance,
        exchange,
        Some(Leverage::Isolated(25)),
        lock_modifier,
        price_level_modifiers,
        performance_arc.clone(),
    );
    let strategy_arc = Arc::new(Mutex::new(strategy));

    let data_feed = MarketDataFeed::new(
        [BTCUSDT, ARBUSDT],
        bar_length,
        initial_data_offset,
        log_level.clone(),
        strategy_arc.clone(),
        performance_arc.clone(),
    );

    let trader = Trader::new(
        &symbols,
        50.0,
        data_feed,
        strategy_arc,
        performance_arc,
        &log_level,
    );

    let signal_subject: BehaviorSubject<Option<SignalCategory>> = BehaviorSubject::new(None);

    let _ = trader.init(signal_subject).await;
}
