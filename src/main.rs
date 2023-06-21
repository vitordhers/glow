// use tokio::*;
extern crate dotenv;
use std::sync::Arc;

use dotenv::dotenv;
pub mod shared;
mod trader;
use log::*;
use tokio::sync::watch::channel;
use trader::{
    enums::log_level::LogLevel,
    indicators::{
        ExponentialMovingAverageIndicator, StochasticIndicator, StochasticThresholdIndicator,
    },
    models::{
        indicator::Indicator, market_data::MarketDataFeed, performance::Performance,
        signal::Signer, strategy::Strategy,
    },
    signals::{
        MultipleStochasticWithThresholdCloseLongSignal,
        MultipleStochasticWithThresholdCloseShortSignal, MultipleStochasticWithThresholdLongSignal,
        MultipleStochasticWithThresholdShortSignal,
    },
    Trader,
};

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();
    let anchor_symbol = String::from("BTCUSDT");
    let symbol = String::from("AGIXUSDT");
    let symbols = [anchor_symbol.clone(), symbol];

    let trend_col = String::from("bullish_market");

    let windows = vec![5, 9, 12];
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 2;

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

    let bar_length = 60;
    let log_level = LogLevel::All;
    let initial_data_offset = 180; // TODO: create a way to calculate this automatically

    // let (mdf_tx, mdf_rx) = channel(MarketDataFeedDTE::default());
    // let (strategy_tx, strategy_rx) = channel(StrategyDTE::default());

    let data_feed = MarketDataFeed::new(
        symbols.clone(),
        bar_length,
        initial_data_offset,
        log_level.clone(),
        // mdf_tx,
    );

    let strategy = Strategy::new(
        "Stochastic Strategy".into(),
        Some(Box::new(ewm_preindicator)),
        indicators,
        signals,
        // strategy_tx,
        // mdf_rx,
    );

    let performance = Performance::default();

    let trader = Trader::new(&symbols, 50.0, data_feed, strategy, performance, &log_level);

    let _ = trader.init().await;

    // data_feed.init().await;

    // let _ = strategy.listen_events().await;
}
