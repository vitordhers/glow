// use tokio::*;
extern crate dotenv;
use dotenv::dotenv;
mod trader;
pub mod shared;
use log::*;
use trader::{
    enums::log_level::LogLevel,
    indicators::{
        ExponentialMovingAverageIndicator, StochasticIndicator, StochasticThresholdIndicator,
    },
    models::{indicator::Indicator, signal::Signer, strategy::Strategy},
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

    let indicators: Vec<Box<(dyn Indicator)>> = vec![
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

    let signals: Vec<Box<dyn Signer>> = vec![
        Box::new(multiple_stochastic_with_threshold_short_signal),
        Box::new(multiple_stochastic_with_threshold_long_signal),
        Box::new(multiple_stochastic_with_threshold_close_short_signal),
        Box::new(multiple_stochastic_with_threshold_close_long_signal),
    ];
    let mut strategy = Strategy::new(
        "Stochastic Strategy".into(),
        Some(Box::new(ewm_preindicator)),
        indicators,
        signals,
    );
    let mut trader = Trader::new(&symbols, 60, 0, 50, &mut strategy, LogLevel::All);

    let connection = trader.market_data_feed.connect().await;

    match connection {
        Err(e) => {
            error!("error found {:?}", e);
        }
        Ok(()) => {
            let _ = trader.market_data_feed.subscribe().await;
            let _events = trader.market_data_feed.listen_events().await;
        }
    }
}
