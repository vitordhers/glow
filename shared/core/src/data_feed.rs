use chrono::{DateTime, Utc};
use common::enums::trading_data_update::TradingDataUpdate;
use common::structs::{Symbol, TradingSettings};
use common::{structs::BehaviorSubject, traits::exchange::DataProviderExchange};
use exchanges::enums::DataProviderExchangeWrapper;
use glow_error::GlowError;
use polars::prelude::*;
use std::sync::{Arc, Mutex, RwLock};
use strategy::Strategy;
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct DataFeed {
    benchmark_datetimes: (Option<DateTime<Utc>>, Option<DateTime<Utc>>), // (start, end)
    data_provider_exchange: DataProviderExchangeWrapper,
    kline_data_listener: BehaviorSubject<TradingDataUpdate>,
    run_benchmark_only: bool, // TODO check if this is really necessary
    pub minimum_klines_for_benchmarking: Arc<RwLock<u32>>,
    pub strategy: Strategy,
    pub strategy_data_emitter: BehaviorSubject<TradingDataUpdate>,
    pub trading_data: Arc<Mutex<DataFrame>>,
    pub trading_data_schema: Schema,
    unique_symbols: Vec<&'static Symbol>,
}

impl DataFeed {
    fn insert_kline_fields(schema_fields: &mut Vec<Field>, unique_symbols: &[&Symbol]) {
        for symbol in unique_symbols {
            let (open_col, high_col, low_col, close_col) = symbol.get_ohlc_cols();
            schema_fields.push(Field::new(open_col.into(), DataType::Float64));
            schema_fields.push(Field::new(high_col.into(), DataType::Float64));
            schema_fields.push(Field::new(low_col.into(), DataType::Float64));
            schema_fields.push(Field::new(close_col.into(), DataType::Float64));
        }
    }

    fn insert_indicators_fields(schema_fields: &mut Vec<Field>, strategy: &Strategy) {
        let columns = strategy.get_indicators_columns();

        for (name, dtype) in columns {
            let field = Field::new(name.as_str().into(), dtype.clone());
            schema_fields.push(field);
        }
    }

    fn insert_signals_fields(schema_fields: &mut Vec<Field>, strategy: &Strategy) {
        for (name, dtype) in strategy.get_signals_columns() {
            let field = Field::new(name.as_str().into(), dtype);
            schema_fields.push(field);
        }
    }

    fn insert_trading_fields(schema_fields: &mut Vec<Field>) -> Schema {
        schema_fields.push(Field::new("trade_fees".into(), DataType::Float64));
        schema_fields.push(Field::new("units".into(), DataType::Float64));
        schema_fields.push(Field::new("profit_and_loss".into(), DataType::Float64));
        schema_fields.push(Field::new("returns".into(), DataType::Float64));
        schema_fields.push(Field::new("balance".into(), DataType::Float64));
        schema_fields.push(Field::new("position".into(), DataType::Int32));
        schema_fields.push(Field::new("action".into(), DataType::String));
        Schema::from_iter(schema_fields.clone())
    }

    fn update_trading_data_df(trading_data: &Arc<Mutex<DataFrame>>, trading_data_df: &DataFrame) {
        let mut trading_data_lock = trading_data.lock().unwrap();
        *trading_data_lock = trading_data_df.clone();
    }

    fn update_minimum_klines_for_benchmarking(
        minimum_klines_for_benchmarking: &Arc<RwLock<u32>>,
        value: u32,
    ) {
        let mut lock = minimum_klines_for_benchmarking.write().unwrap();
        *lock = value;
    }

    fn set_schema(strategy: &Strategy, unique_symbols: &[&Symbol]) -> (Schema, DataFrame, u32) {
        let mut schema_fields = vec![Field::new(
            "start_time".into(),
            DataType::Datetime(TimeUnit::Milliseconds, None),
        )];
        Self::insert_kline_fields(&mut schema_fields, unique_symbols);
        Self::insert_indicators_fields(&mut schema_fields, strategy);
        Self::insert_signals_fields(&mut schema_fields, strategy);
        let minimum_klines_for_benchmarking = strategy.get_minimum_klines_for_calculation();
        let trading_data_schema = Self::insert_trading_fields(&mut schema_fields);
        let trading_data_df = DataFrame::empty_with_schema(&trading_data_schema);
        (
            trading_data_schema,
            trading_data_df,
            minimum_klines_for_benchmarking,
        )
    }

    pub fn new(
        benchmark_datetimes: (Option<DateTime<Utc>>, Option<DateTime<Utc>>),
        data_provider_exchange: DataProviderExchangeWrapper,
        run_benchmark_only: bool,
        strategy: &Strategy,
        trading_settings: &TradingSettings,
    ) -> DataFeed {
        if let (Some(benchmark_start), Some(benchmark_end)) = benchmark_datetimes {
            assert!(
                benchmark_start < benchmark_end,
                "Benchmark start must be before benchmark end"
            );
        }

        let trading_data = Arc::new(Mutex::new(DataFrame::empty()));
        let unique_symbols = &trading_settings.get_unique_symbols();
        let (trading_data_schema, trading_data_df, minimum_klines_for_benchmarking) =
            Self::set_schema(strategy, unique_symbols);
        Self::update_trading_data_df(&trading_data, &trading_data_df);
        let strategy_data_emitter = BehaviorSubject::new(TradingDataUpdate::default());
        let kline_data_listener = data_provider_exchange.get_kline_data_emitter().clone();

        DataFeed {
            benchmark_datetimes,
            data_provider_exchange,
            run_benchmark_only,
            kline_data_listener,
            minimum_klines_for_benchmarking: Arc::new(RwLock::new(minimum_klines_for_benchmarking)),
            strategy: strategy.clone(),
            strategy_data_emitter,
            trading_data,
            trading_data_schema,
            unique_symbols: unique_symbols.to_vec(),
        }
    }

    /// this must be run before init
    pub fn patch_benchmark_datetimes(
        &mut self,
        benchmark_start: Option<DateTime<Utc>>,
        benchmark_end: Option<DateTime<Utc>>,
    ) {
        self.benchmark_datetimes = (benchmark_start, benchmark_end)
    }

    pub fn patch_trading_settings(&mut self, trading_settings: &TradingSettings) {
        self.data_provider_exchange.patch_settings(trading_settings);
        let unique_symbols = trading_settings.symbols_pair.get_unique_symbols();
        let (trading_data_schema, trading_data_df, minimum_klines_for_benchmarking) =
            Self::set_schema(&self.strategy, &unique_symbols);
        Self::update_trading_data_df(&self.trading_data, &trading_data_df);
        Self::update_minimum_klines_for_benchmarking(
            &self.minimum_klines_for_benchmarking,
            minimum_klines_for_benchmarking,
        );
        self.trading_data_schema = trading_data_schema;
    }

    pub fn patch_strategy(&mut self, strategy: &Strategy) {
        self.data_provider_exchange.patch_strategy(strategy);
        let (trading_data_schema, trading_data_df, minimum_klines_for_benchmarking) =
            Self::set_schema(strategy, &self.unique_symbols);
        Self::update_trading_data_df(&self.trading_data, &trading_data_df);
        Self::update_minimum_klines_for_benchmarking(
            &self.minimum_klines_for_benchmarking,
            minimum_klines_for_benchmarking,
        );
        self.trading_data_schema = trading_data_schema;
    }

    fn set_initial_strategy_data(
        &self,
        initial_klines_lf: LazyFrame,
    ) -> Result<LazyFrame, GlowError> {
        let initial_strategy_lf = self.strategy.append_indicators_to_lf(initial_klines_lf)?;
        let initial_strategy_lf = self.strategy.append_signals_to_lf(initial_strategy_lf)?;
        let initial_strategy_lf = initial_strategy_lf.cache();
        Ok(initial_strategy_lf)
    }

    fn handle_initial_klines(&self, initial_klines_df: DataFrame) -> Result<DataFrame, GlowError> {
        let initial_klines_lf = initial_klines_df.lazy();
        let initial_klines_lf = self.set_initial_strategy_data(initial_klines_lf)?;
        let initial_strategy_data = initial_klines_lf.collect()?;
        Ok(initial_strategy_data)
    }

    fn update_strategy_data(&self, market_klines_df: DataFrame) -> Result<DataFrame, GlowError> {
        let trading_data: DataFrame;

        {
            let trading_data_lock = self.trading_data.lock().unwrap();
            trading_data = trading_data_lock.clone();
        }
        let updated_strategy_data = trading_data.vstack(&market_klines_df)?;

        let updated_strategy_data = self
            .strategy
            .append_indicators_to_df(updated_strategy_data)?;
        let updated_strategy_data = self.strategy.append_signals_to_df(updated_strategy_data)?;
        Ok(updated_strategy_data)
    }

    fn handle_market_klines(&self, market_klines_df: DataFrame) -> Result<DataFrame, GlowError> {
        let updated_strategy_df = self.update_strategy_data(market_klines_df)?;
        Ok(updated_strategy_df)
    }

    fn init_kline_data_handler(&self) -> JoinHandle<()> {
        let data_feed = self.clone();
        spawn(async move {
            let mut subscription = data_feed.kline_data_listener.subscribe();
            while let Some(klines_data) = subscription.next().await {
                match klines_data {
                    TradingDataUpdate::Initial(initial_klines_df) => {
                        match data_feed.handle_initial_klines(initial_klines_df) {
                            Ok(initial_strategy_df) => {
                                let payload = TradingDataUpdate::Initial(initial_strategy_df);
                                data_feed.strategy_data_emitter.next(payload);
                            }
                            Err(error) => {
                                println!("handle_initial_klines error {:?}", error);
                            }
                        }
                    }
                    TradingDataUpdate::Market(market_klines_df) => {
                        match data_feed.handle_market_klines(market_klines_df) {
                            Ok(updated_strategy_df) => {
                                let payload = TradingDataUpdate::Market(updated_strategy_df);
                                data_feed.strategy_data_emitter.next(payload);
                            }
                            Err(error) => {
                                println!("handle_market_klines error {:?}", error);
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
    }

    fn init_data_provider_handler(&self) -> JoinHandle<()> {
        let mut data_provider_binding = self.data_provider_exchange.clone();
        let run_benchmark_only = self.run_benchmark_only;
        let trading_data_schema = self.trading_data_schema.clone();
        let benchmark_start = self.benchmark_datetimes.0;
        let benchmark_end = self.benchmark_datetimes.1;

        spawn(async move {
            let _ = data_provider_binding
                .init(
                    benchmark_start,
                    benchmark_end,
                    run_benchmark_only,
                    trading_data_schema,
                )
                .await;
        })
    }

    pub fn init(&self) {
        self.init_kline_data_handler();
        self.init_data_provider_handler();
    }
}
