use chrono::NaiveDateTime;
use common::structs::Symbol;
use common::{
    enums::{balance::Balance, order_action::OrderAction, trading_data_update::TradingDataUpdate},
    structs::{BehaviorSubject, Execution, Trade},
    traits::exchange::{DataProviderExchange, TraderExchange, TraderHelper},
};
use exchanges::enums::{DataProviderExchangeWrapper, TraderExchangeWrapper};
use glow_error::GlowError;
use polars::prelude::*;
use std::sync::{Arc, Mutex};
use strategy::Strategy;
use tokio::spawn;

#[derive(Clone)]
pub struct DataFeed {
    benchmark_datetimes: (Option<NaiveDateTime>, Option<NaiveDateTime>), // (start, end)
    pub current_trade_listener: BehaviorSubject<Option<Trade>>,
    pub data_provider_exchange: DataProviderExchangeWrapper,
    pub data_provider_last_ws_error_ts: Arc<Mutex<Option<i64>>>,
    pub run_benchmark_only: bool,
    pub kline_data_schema: Schema,
    pub minimum_klines_for_benchmarking: u32,
    pub strategy: Strategy,
    pub trading_data_schema: Schema,
    pub trading_data_update_listener: BehaviorSubject<TradingDataUpdate>,
    pub trader_exchange_listener: BehaviorSubject<TraderExchangeWrapper>,
    pub update_balance_listener: BehaviorSubject<Option<Balance>>,
    pub update_executions_listener: BehaviorSubject<Vec<Execution>>,
    pub update_order_listener: BehaviorSubject<Option<OrderAction>>,
}

impl DataFeed {
    fn insert_kline_fields(
        schema_fields: &mut Vec<Field>,
        unique_symbols: &Vec<&Symbol>,
    ) -> Schema {
        for symbol in unique_symbols {
            let (open_col, high_col, low_col, close_col) = symbol.get_ohlc_cols();
            schema_fields.push(Field::new(&open_col, DataType::Float64));
            schema_fields.push(Field::new(&high_col, DataType::Float64));
            schema_fields.push(Field::new(&close_col, DataType::Float64));
            schema_fields.push(Field::new(&low_col, DataType::Float64));
        }

        Schema::from_iter(schema_fields.clone().into_iter())
    }

    fn insert_indicators_fields(schema_fields: &mut Vec<Field>, strategy: &Strategy) {
        let columns = strategy.get_indicators_columns();

        for (name, dtype) in columns {
            let field = Field::new(name.as_str(), dtype.clone());
            schema_fields.push(field);
        }
    }

    fn insert_signals_fields(schema_fields: &mut Vec<Field>, strategy: &Strategy) {
        for (name, dtype) in strategy.get_signals_columns() {
            let field = Field::new(name.as_str(), dtype);
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
        benchmark_datetimes: (Option<NaiveDateTime>, Option<NaiveDateTime>),
        current_trade_listener: &BehaviorSubject<Option<Trade>>,
        data_provider_last_ws_error_ts: &Arc<Mutex<Option<i64>>>,
        data_provider_exchange: DataProviderExchangeWrapper,
        run_benchmark_only: bool,
        strategy: &Strategy,
        trading_data_update_listener: &BehaviorSubject<TradingDataUpdate>,
        trader_exchange_listener: &BehaviorSubject<TraderExchangeWrapper>,
        update_balance_listener: &BehaviorSubject<Option<Balance>>,
        update_executions_listener: &BehaviorSubject<Vec<Execution>>,
        update_order_listener: &BehaviorSubject<Option<OrderAction>>,
    ) -> DataFeed {
        if let (Some(benchmark_start), Some(benchmark_end)) = benchmark_datetimes {
            assert!(
                benchmark_start < benchmark_end,
                "Benchmark start must be before benchmark end"
            );
        }

        let mut schema_fields = vec![Field::new(
            "start_time",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        )];

        let trader_exchange = trader_exchange_listener.value();
        let trading_settings = trader_exchange.get_trading_settings();
        let unique_symbols = trading_settings.get_unique_symbols();

        let kline_data_schema = Self::insert_kline_fields(&mut schema_fields, &unique_symbols);

        Self::insert_indicators_fields(&mut schema_fields, &strategy);
        Self::insert_signals_fields(&mut schema_fields, &strategy);

        let minimum_klines_for_benchmarking = strategy.get_minimum_klines_for_calculation();

        let trading_data_schema = Self::insert_performance_fields(&mut schema_fields);

        DataFeed {
            benchmark_datetimes,
            current_trade_listener: current_trade_listener.clone(),
            data_provider_exchange,
            data_provider_last_ws_error_ts: data_provider_last_ws_error_ts.clone(),
            run_benchmark_only,
            kline_data_schema,
            minimum_klines_for_benchmarking,
            strategy: strategy.clone(),
            trading_data_schema,
            trading_data_update_listener: trading_data_update_listener.clone(),
            trader_exchange_listener: trader_exchange_listener.clone(),
            update_balance_listener: update_balance_listener.clone(),
            update_executions_listener: update_executions_listener.clone(),
            update_order_listener: update_order_listener.clone(),
        }
    }

    pub async fn init(&'static mut self) -> Result<(), GlowError> {
        let mut data_provider_binding = self.data_provider_exchange.clone();
        let kline_data_schema = self.kline_data_schema.clone();
        let run_benchmark_only = self.run_benchmark_only;
        let trading_data_schema = self.trading_data_schema.clone();
        let benchmark_start = self.benchmark_datetimes.0;
        let benchmark_end = self.benchmark_datetimes.1;

        let data_provider_handle = spawn(async move {
            let _ = data_provider_binding
                .init(
                    benchmark_start,
                    benchmark_end,
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
        // TODO: move to trader?
        let mut trader_binding = self.trader_exchange_listener.value();
        let trader_handle = spawn(async move { trader_binding.init().await });
        let _ = trader_handle.await;

        Ok(())
    }

    pub fn set_strategy_data(&self, initial_klines_lf: LazyFrame) -> Result<LazyFrame, GlowError> {
        let initial_strategy_lf = self.strategy.append_indicators_to_lf(initial_klines_lf)?;
        let initial_strategy_lf = self.strategy.append_signals_to_lf(initial_strategy_lf)?;
        let initial_strategy_lf = initial_strategy_lf.cache();
        Ok(initial_strategy_lf)
    }

    pub fn update_strategy_data(
        &self,
        current_trading_data: DataFrame,
        last_kline_data: DataFrame,
    ) -> Result<DataFrame, GlowError> {
        let updated_strategy_data = current_trading_data.vstack(&last_kline_data)?;

        let updated_strategy_data = self
            .strategy
            .append_indicators_to_df(updated_strategy_data)?;
        let updated_strategy_data = self.strategy.append_signals_to_df(updated_strategy_data)?;
        Ok(updated_strategy_data)
    }
}
