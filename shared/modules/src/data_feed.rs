use chrono::{Duration as ChronoDuration, NaiveDateTime};
use common::traits::indicator::Indicator;
use common::traits::signal::Signal;
use common::{
    enums::{
        balance::Balance, log_level::LogLevel, order_action::OrderAction,
        trading_data_update::TradingDataUpdate,
    },
    functions::get_symbol_ohlc_cols,
    structs::{BehaviorSubject, Execution, Trade},
    traits::exchange::{DataProviderExchange, TraderExchange, TraderHelper},
};
use exchanges::enums::{DataProviderExchangeWrapper, TraderExchangeWrapper};
use glow_error::GlowError;
use polars::prelude::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use strategy::indicators::IndicatorWrapper;
use strategy::preindicators::PreIndicatorWrapper;
use strategy::signals::SignalWrapper;
use strategy::Strategy;
use tokio::spawn;

#[derive(Clone)]
pub struct DataFeed {
    pub current_trade_listener: BehaviorSubject<Option<Trade>>,
    pub data_provider_exchange: DataProviderExchangeWrapper,
    pub data_provider_exchange_socket_error_ts: Arc<Mutex<Option<i64>>>,
    pub is_test_mode: bool,
    pub kline_duration: Duration,
    pub kline_data_schema: Schema,
    pub log_level: LogLevel,
    pub trades_start: NaiveDateTime,
    pub minimum_klines_for_benchmarking: u32,
    pub trading_data_schema: Schema,
    pub trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
    pub trading_data_listener: BehaviorSubject<DataFrame>,
    pub trader_exchange_listener: BehaviorSubject<TraderExchangeWrapper>,
    pub unique_symbols: Vec<&'static str>,
    pub update_balance_listener: BehaviorSubject<Option<Balance>>,
    pub update_executions_listener: BehaviorSubject<Vec<Execution>>,
    pub update_order_listener: BehaviorSubject<Option<OrderAction>>,
}

impl DataFeed {
    fn insert_kline_fields(schema_fields: &mut Vec<Field>, unique_symbols: &Vec<&str>) -> Schema {
        for symbol in unique_symbols {
            let (open_col, high_col, low_col, close_col) = get_symbol_ohlc_cols(&symbol);
            schema_fields.push(Field::new(&open_col, DataType::Float64));
            schema_fields.push(Field::new(&high_col, DataType::Float64));
            schema_fields.push(Field::new(&close_col, DataType::Float64));
            schema_fields.push(Field::new(&low_col, DataType::Float64));
        }

        Schema::from_iter(schema_fields.clone().into_iter())
    }

    fn insert_preindicators_fields(
        schema_fields: &mut Vec<Field>,
        preindicators: &Vec<PreIndicatorWrapper>,
    ) -> u32 {
        let mut preindicators_minimum_klines_for_benchmarking: Vec<u32> = vec![];

        for preindicator in preindicators {
            let preindicator_minimum_klines_for_benchmarking =
                preindicator.get_minimum_klines_for_benchmarking();
            preindicators_minimum_klines_for_benchmarking
                .push(preindicator_minimum_klines_for_benchmarking);
            let columns = preindicator.get_indicator_columns();
            for (name, dtype) in columns {
                let field = Field::new(&name, dtype);
                schema_fields.push(field);
            }
        }

        preindicators_minimum_klines_for_benchmarking
            .into_iter()
            .max()
            .unwrap_or_default()
    }

    fn insert_indicators_fields(
        schema_fields: &mut Vec<Field>,
        indicators: &Vec<IndicatorWrapper>,
    ) -> u32 {
        let mut indicators_minimum_klines_for_benchmarking: Vec<u32> = vec![];

        for indicator in indicators {
            let indicator_minimum_klines_for_benchmarking =
                indicator.get_minimum_klines_for_benchmarking();
            indicators_minimum_klines_for_benchmarking
                .push(indicator_minimum_klines_for_benchmarking);
            let columns = indicator.get_indicator_columns();
            for (name, dtype) in columns {
                let field = Field::new(&name, dtype);
                schema_fields.push(field);
            }
        }

        indicators_minimum_klines_for_benchmarking
            .into_iter()
            .max()
            .unwrap_or_default()
    }

    fn insert_signals_fields(schema_fields: &mut Vec<Field>, signals: &Vec<SignalWrapper>) {
        for signal in signals {
            let field = Field::new(signal.signal_category().get_column(), DataType::Int32);
            schema_fields.push(field);
        }
    }

    fn insert_performance_fields(schema_fields: &mut Vec<Field>) -> Schema {
        schema_fields.push(Field::new("trade_fees", DataType::Float64));
        schema_fields.push(Field::new("units", DataType::Float64));
        schema_fields.push(Field::new("profit_and_loss", DataType::Float64));
        schema_fields.push(Field::new("returns", DataType::Float64));
        schema_fields.push(Field::new("balance", DataType::Float64));
        schema_fields.push(Field::new("position", DataType::Int32));
        schema_fields.push(Field::new("action", DataType::Utf8));
        Schema::from_iter(schema_fields.clone().into_iter())
    }

    pub fn new(
        current_trade_listener: &BehaviorSubject<Option<Trade>>,
        data_provider_exchange_socket_error_ts: &Arc<Mutex<Option<i64>>>,
        data_provider_exchange: DataProviderExchangeWrapper,
        initial_datetime: NaiveDateTime,
        is_test_mode: bool,
        kline_duration_in_seconds: u64,
        log_level: LogLevel,
        strategy: &Strategy,
        trading_data_update_listener: &BehaviorSubject<TradingDataUpdate>,
        trading_data_listener: &BehaviorSubject<DataFrame>,
        trader_exchange_listener: &BehaviorSubject<TraderExchangeWrapper>,
        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
    ) -> DataFeed {
        let mut schema_fields = vec![Field::new(
            "start_time",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        )];

        let trader_exchange = trader_exchange_listener.value();
        let trading_settings = trader_exchange.get_trading_settings();
        let unique_symbols = trading_settings.get_unique_symbols();

        let kline_data_schema = Self::insert_kline_fields(&mut schema_fields, &unique_symbols);
        let preindicators_minimum_klines_for_benchmarking =
            Self::insert_preindicators_fields(&mut schema_fields, &strategy.preindicators);
        let indicators_minimum_klines_for_benchmarking =
            Self::insert_indicators_fields(&mut schema_fields, &strategy.indicators);
        let minimum_klines_for_benchmarking = vec![
            preindicators_minimum_klines_for_benchmarking,
            indicators_minimum_klines_for_benchmarking,
        ]
        .iter()
        .cloned()
        .max()
        .unwrap_or_default();
        Self::insert_signals_fields(&mut schema_fields, &strategy.signals);
        let trading_data_schema = Self::insert_performance_fields(&mut schema_fields);

        let kline_duration = Duration::from_secs(kline_duration_in_seconds);
        // let current_datetime = current_datetime();

        let minute_lasting_seconds = initial_datetime.timestamp() % 60;
        let seconds_to_next_full_minute = if minute_lasting_seconds == 0 {
            0
        } else {
            60 - minute_lasting_seconds
        };

        let trades_start = initial_datetime + ChronoDuration::seconds(seconds_to_next_full_minute);
        if !is_test_mode {
            println!(
                "seconds_to_next_full_minute {:?}",
                seconds_to_next_full_minute
            );
            println!(
                "{} | 💹 Initializing DataFeed -> trades might be open after {:?}",
                initial_datetime, trades_start
            );
        }

        // let data_provider_exchange = DataProviderExchangeWrapper::new_binance_data_provider(
        //     trades_start,
        //     &kline_data_schema,
        //     kline_duration,
        //     data_provider_exchange_socket_error_ts,
        //     minimum_klines_for_benchmarking,
        //     trading_settings.symbols,
        //     &trading_data_schema,
        //     &trading_data_update_listener,
        // );

        DataFeed {
            current_trade_listener: current_trade_listener.clone(),
            data_provider_exchange,
            data_provider_exchange_socket_error_ts: data_provider_exchange_socket_error_ts.clone(),
            is_test_mode,
            kline_data_schema,
            kline_duration,
            log_level,
            trades_start,
            minimum_klines_for_benchmarking,
            trading_data_schema,
            trading_data_update_listener: trading_data_update_listener.clone(),
            trading_data_listener: trading_data_listener.clone(),
            trader_exchange_listener: trader_exchange_listener.clone(),
            unique_symbols,
            update_balance_listener: update_balance_listener.clone(),
            update_executions_listener: update_executions_listener.clone(),
            update_order_listener: update_order_listener.clone(),
        }
    }

    pub async fn init(
        &'static mut self,
        benchmark_end: NaiveDateTime,
        benchmark_start: Option<NaiveDateTime>,
    ) -> Result<(), GlowError> {
        let mut data_provider_binding = self.data_provider_exchange.clone();
        let kline_data_schema = self.kline_data_schema.clone();
        let run_benchmark_only = self.is_test_mode;
        let trading_data_schema = self.trading_data_schema.clone();

        let data_provider_handle = spawn(async move {
            let _ = data_provider_binding
                .init(
                    benchmark_end,
                    benchmark_start,
                    kline_data_schema,
                    run_benchmark_only,
                    trading_data_schema,
                )
                .await;
        });
        let _ = data_provider_handle.await;

        if run_benchmark_only {
            return Ok(());
        }

        let mut trader_binding = self.trader_exchange_listener.value();
        let trader_handle = spawn(async move { trader_binding.init().await });
        let _ = trader_handle.await;

        Ok(())
    }
}
