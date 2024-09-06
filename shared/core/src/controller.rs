use crate::{data_feed::DataFeed, trader::Trader};

use super::performance::Performance;
use chrono::NaiveDateTime;
use common::{
    enums::{balance::Balance, trading_data_update::TradingDataUpdate},
    functions::current_datetime,
    structs::{BehaviorSubject, SymbolsPair, Trade, TradingSettings},
};
use exchanges::enums::{
    DataProviderExchangeId, DataProviderExchangeWrapper, TraderExchangeId, TraderExchangeWrapper,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
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
        let default_symbols_pair = SymbolsPair::default();
        let default_trading_settings = TradingSettings::load_or_default();

        let default_strategy = Strategy::default();

        let default_datetimes = (None::<NaiveDateTime>, Some(current_datetime()));

        let default_data_provider_exchange_id = DataProviderExchangeId::default();
        let data_provider_last_ws_error_ts: Arc<Mutex<Option<i64>>> = Arc::new(Mutex::new(None));
        let klines_data_bs = BehaviorSubject::new(TradingDataUpdate::default());
        let strategy_data_bs = BehaviorSubject::new(TradingDataUpdate::default());
        let default_data_provider_exchange = DataProviderExchangeWrapper::new(
            default_data_provider_exchange_id,
            default_trading_settings.granularity.get_chrono_duration(),
            &data_provider_last_ws_error_ts,
            default_strategy.get_minimum_klines_for_calculation(),
            default_symbols_pair,
            &klines_data_bs,
        );

        let data_feed = DataFeed::new(
            default_datetimes,
            default_data_provider_exchange,
            &klines_data_bs,
            true,
            &default_strategy,
            &strategy_data_bs,
            &default_trading_settings.get_unique_symbols(),
        );

        let default_trader_exchange_id = TraderExchangeId::default();
        let selected_trader_exchange_id = BehaviorSubject::new(default_trader_exchange_id);
        let current_trade_listener: BehaviorSubject<Option<Trade>> = BehaviorSubject::new(None);
        let current_balance_listener = BehaviorSubject::new(Balance::default());
        let update_executions_listener = BehaviorSubject::new(vec![]);
        let update_order_listener = BehaviorSubject::new(None);

        let default_trader_exchange = TraderExchangeWrapper::new(
            default_trader_exchange_id,
            &current_trade_listener,
            &default_trading_settings,
            &current_balance_listener,
            &update_executions_listener,
            &update_order_listener,
        );

        let trader = Trader::new(
            &current_balance_listener,
            current_trade_listener,
            exchange_listener,
            performance_data_emitter,
            strategy_data_listener,
            trading_data,
            trading_data_klines_limit,
            update_balance_listener,
            update_order_listener,
            update_executions_listener,
        );

        let performance = Performance::new(trading_settings, traded_data_listener);

        Self {
            data_feed: data_feed.clone(),
            performance: performance.clone(),
            trader: trader.clone(),
        }
    }

    pub async fn init(&self) {
        // init core modules
    }
}
