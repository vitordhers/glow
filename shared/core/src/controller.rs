use crate::{config::BenchmarkSettings, data_feed::DataFeed, trader::Trader};

use super::performance::Performance;
use chrono::{Duration, NaiveDateTime};
use common::structs::TradingSettings;
use common::traits::exchange::TraderHelper;
use exchanges::enums::{DataProviderExchangeWrapper, TraderExchangeWrapper};
use strategy::{Strategy, StrategyId};

#[derive(Clone)]
pub struct Controller {
    pub benchmark_settings: BenchmarkSettings,
    pub data_feed: DataFeed,
    pub performance: Performance,
    pub trader: Trader,
}

impl Controller {
    pub fn new(run_benchmark_only: bool) -> Self {
        let benchmark_settings = BenchmarkSettings::load_or_default();
        let BenchmarkSettings {
            datetimes,
            strategy_id,
            data_provider_id,
            trader_exchange_id,
        } = benchmark_settings;
        let trading_settings = TradingSettings::load_or_default();
        let strategy = Strategy::new(strategy_id, trading_settings.symbols_pair);

        let default_data_provider_exchange =
            DataProviderExchangeWrapper::new(data_provider_id, &strategy, &trading_settings);

        let data_feed = DataFeed::new(
            datetimes,
            default_data_provider_exchange,
            run_benchmark_only,
            &strategy,
            &trading_settings,
        );

        let default_trader_exchange =
            TraderExchangeWrapper::new(trader_exchange_id, &trading_settings);

        let trader = Trader::new(
            &data_feed.strategy_data_emitter,
            default_trader_exchange,
            &data_feed.trading_data,
            &data_feed.minimum_klines_for_benchmarking,
        );

        let initial_datetime = datetimes.1.unwrap() + Duration::days(1);

        let performance = Performance::new(
            initial_datetime,
            &trading_settings,
            &trader.performance_data_emitter,
        );

        Self {
            benchmark_settings,
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
        self.benchmark_settings.datetimes = (benchmark_start, benchmark_end);
        let _ = self.benchmark_settings.save_config();
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

    pub fn patch_strategy_id(&mut self, strategy_id: StrategyId) {
        self.benchmark_settings.strategy_id = strategy_id;
        let _ = self.benchmark_settings.save_config();
        let symbols_pair = self
            .trader
            .trader_exchange
            .get_trading_settings()
            .symbols_pair;
        let updated_strategy = Strategy::new(strategy_id, symbols_pair);
        self.data_feed.patch_strategy(&updated_strategy);
    }

    pub fn init(&self) {
        self.performance.init();
        self.trader.init();
        self.data_feed.init();
    }
}
