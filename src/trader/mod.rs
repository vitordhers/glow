use chrono::NaiveDateTime;
use enums::log_level::LogLevel;
use models::{market_data::MarketDataFeed, performance::Performance, strategy::Strategy};
use polars::prelude::*;
use tokio::task::JoinError;

mod constants;
pub mod enums;
pub mod errors;
mod functions;
pub mod indicators;
pub mod models;
pub mod signals;
pub mod contracts;
#[derive(Clone)]
pub struct Trader {
    pub symbols: [String; 2],
    pub units: i64,
    pub initial_balance: f64,
    pub current_balance: f64,
    pub market_data_feed: MarketDataFeed,
    pub strategy: Strategy,
    pub performance: Performance,
}

impl Trader {
    pub fn new(
        symbols: &[String; 2],
        initial_balance: f64,
        market_data_feed: MarketDataFeed,
        strategy: Strategy,
        performance: Performance,
        log_level: &LogLevel,
    ) -> Trader {
        Trader {
            symbols: symbols.clone(),
            initial_balance,
            current_balance: initial_balance,
            units: 0,
            market_data_feed,
            strategy,
            performance,
        }
    }

    pub async fn init(mut self) -> Result<(), JoinError> {
        let cloned_data_feed = self.market_data_feed.clone();
        let mut cloned_strategy = self.strategy.clone();
        let mut cloned_performance = self.performance.clone();
        let handle1 = tokio::spawn(async move {
            cloned_data_feed
                .fetch_benchmark_data(
                    &mut cloned_strategy,
                    &mut cloned_performance,
                    self.initial_balance,
                )
                .await;
        });

        let handle2 = tokio::spawn(async move {
            self.market_data_feed
                .init(
                    &mut self.strategy,
                    &mut self.performance,
                    self.initial_balance,
                )
                .await;
        });

        let _ = handle1.await;

        handle2.await
    }
}
