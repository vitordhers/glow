use crate::{data_feed::DataFeed, trader::Trader};

use super::performance::Performance;
use chrono::{Duration, NaiveDateTime};
use common::{functions::current_datetime, structs::TradingSettings};
use exchanges::enums::{
    DataProviderExchangeId, DataProviderExchangeWrapper, TraderExchangeId, TraderExchangeWrapper,
};
use strategy::Strategy;

#[derive(Clone)]
pub struct Controller {
    pub data_feed: DataFeed,
    pub performance: Performance,
    pub trader: Trader,
}

impl Controller {
    pub fn new(run_benchmark_only: bool) -> Self {
        let default_trading_settings = TradingSettings::load_or_default();
        let default_strategy = Strategy::default();
        let default_datetimes = (None::<NaiveDateTime>, Some(current_datetime()));

        let default_data_provider_exchange_id = DataProviderExchangeId::default();
        let default_data_provider_exchange = DataProviderExchangeWrapper::new(
            default_data_provider_exchange_id,
            &default_strategy,
            &default_trading_settings,
        );

        let data_feed = DataFeed::new(
            default_datetimes,
            default_data_provider_exchange,
            run_benchmark_only,
            &default_strategy,
            &default_trading_settings,
        );

        let default_trader_exchange_id = TraderExchangeId::default();

        let default_trader_exchange =
            TraderExchangeWrapper::new(default_trader_exchange_id, &default_trading_settings);

        let trader = Trader::new(
            &data_feed.strategy_data_emitter,
            default_trader_exchange,
            &data_feed.trading_data,
            &data_feed.minimum_klines_for_benchmarking,
        );

        let default_initial_datetime = default_datetimes.1.unwrap() + Duration::days(1);

        let performance = Performance::new(
            default_initial_datetime,
            &default_trading_settings,
            &trader.performance_data_emitter,
        );

        Self {
            data_feed,
            performance,
            trader,
        }
    }

    pub fn patch_benchmark_datetimes(
        &mut self,
        benchmark_start: Option<NaiveDateTime>,
        benchmark_end: Option<NaiveDateTime>,
    ) {
        self.data_feed
            .patch_benchmark_datetimes(benchmark_start, benchmark_end);
        self.performance
            .patch_benchmark_datetimes(benchmark_start, benchmark_end);
    }

    pub fn patch_settings(&mut self, trading_settings: &TradingSettings) {
        self.data_feed.patch_trading_settings(trading_settings);
        self.trader.patch_settings(trading_settings);
        self.performance.patch_settings(trading_settings);
        let _ = trading_settings.save_config();
    }

    pub fn patch_strategy(&mut self, strategy: &Strategy) {
        self.data_feed.patch_strategy(strategy);
    }

    pub fn init(&self) {
        self.performance.init();
        self.trader.init();
        self.data_feed.init();
    }
}
