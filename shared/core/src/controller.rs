use crate::{data_feed::DataFeed, trader::Trader};

use super::performance::Performance;
use chrono::NaiveDateTime;
use common::{functions::current_datetime, structs::TradingSettings};
use exchanges::enums::{
    DataProviderExchangeId, DataProviderExchangeWrapper, TraderExchangeId, TraderExchangeWrapper,
};
use std::sync::{Arc, Mutex};
use strategy::Strategy;
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct Controller {
    data_feed: Arc<Mutex<DataFeed>>,
    performance: Arc<Mutex<Performance>>,
    trader: Arc<Mutex<Trader>>,
}

impl Controller {
    pub fn new() -> Self {
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
            true,
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

        let performance =
            Performance::new(&default_trading_settings, &trader.performance_data_emitter);

        Self {
            data_feed: Arc::new(Mutex::new(data_feed)),
            performance: Arc::new(Mutex::new(performance)),
            trader: Arc::new(Mutex::new(trader)),
        }
    }

    pub async fn init(&self) {
        // init core modules
    }
}
