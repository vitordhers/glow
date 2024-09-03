use chrono::NaiveDateTime;
use common::enums::trading_data_update::{KlinesDataUpdate, StrategyDataUpdate};
use common::structs::Symbol;
use common::{
    structs::BehaviorSubject,
    traits::exchange::{DataProviderExchange, TraderHelper},
};
use exchanges::enums::{DataProviderExchangeWrapper, TraderExchangeWrapper};
use glow_error::GlowError;
use polars::prelude::*;
use std::sync::{Arc, Mutex};
use strategy::Strategy;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct DataFeed {
    benchmark_datetimes: (Option<NaiveDateTime>, Option<NaiveDateTime>), // (start, end)
    // pub current_trade_listener: BehaviorSubject<Option<Trade>>, // TODO check if this is really necessary
    data_provider_exchange: DataProviderExchangeWrapper,
    data_provider_last_ws_error_ts: Arc<Mutex<Option<i64>>>,
    kline_data_listener: BehaviorSubject<KlinesDataUpdate>,
    run_benchmark_only: bool, // TODO check if this is really necessary
    // pub kline_data_schema: Schema, // TODO check if this is really necessary
    minimum_klines_for_benchmarking: u32, // TODO check if this is really necessary
    // signal_listener: BehaviorSubject<SignalCategory>,
    strategy: Strategy,
    strategy_data_emitter: BehaviorSubject<StrategyDataUpdate>,
    trading_data: Arc<Mutex<DataFrame>>,
    trading_data_schema: Schema,
    // pub trading_data_update_listener: BehaviorSubject<TradingDataUpdate>, // TODO: deprecate this
    // pub trader_exchange_listener: BehaviorSubject<TraderExchangeWrapper>, // TODO check if this is really necessary
    // pub update_balance_listener: BehaviorSubject<Option<Balance>>, // TODO check if this is really necessary
    // pub update_executions_listener: BehaviorSubject<Vec<Execution>>, // TODO check if this is really necessary
    // pub update_order_listener: BehaviorSubject<Option<OrderAction>>, // TODO check if this is really necessary
}

impl DataFeed {
    fn insert_kline_fields(schema_fields: &mut Vec<Field>, unique_symbols: &Vec<&Symbol>) {
        for symbol in unique_symbols {
            let (open_col, high_col, low_col, close_col) = symbol.get_ohlc_cols();
            schema_fields.push(Field::new(&open_col, DataType::Float64));
            schema_fields.push(Field::new(&high_col, DataType::Float64));
            schema_fields.push(Field::new(&close_col, DataType::Float64));
            schema_fields.push(Field::new(&low_col, DataType::Float64));
        }
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

    fn insert_trading_fields(schema_fields: &mut Vec<Field>) -> Schema {
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
        data_provider_exchange: DataProviderExchangeWrapper,
        data_provider_last_ws_error_ts: &Arc<Mutex<Option<i64>>>,
        kline_data_listener: &BehaviorSubject<KlinesDataUpdate>,
        run_benchmark_only: bool,
        strategy: &Strategy,
        strategy_data_emitter: &BehaviorSubject<StrategyDataUpdate>,
        trader_exchange_listener: BehaviorSubject<TraderExchangeWrapper>,
        trading_data: &Arc<Mutex<DataFrame>>,
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

        Self::insert_kline_fields(&mut schema_fields, &unique_symbols);

        Self::insert_indicators_fields(&mut schema_fields, &strategy);
        Self::insert_signals_fields(&mut schema_fields, &strategy);

        let minimum_klines_for_benchmarking = strategy.get_minimum_klines_for_calculation();

        let trading_data_schema = Self::insert_trading_fields(&mut schema_fields);
        let trading_data = trading_data.clone();
        {
            let mut trading_data_lock = trading_data.lock().unwrap();
            *trading_data_lock = DataFrame::from(&trading_data_schema);
        }

        DataFeed {
            benchmark_datetimes,
            // benchmark_data_emitter: benchmark_data_emitter.clone(),
            // current_trade_listener: current_trade_listener.clone(),
            data_provider_exchange,
            data_provider_last_ws_error_ts: data_provider_last_ws_error_ts.clone(),
            run_benchmark_only,
            kline_data_listener: kline_data_listener.clone(),
            // kline_data_schema,
            minimum_klines_for_benchmarking,
            // signal_listener: signal_listener.clone(),
            strategy: strategy.clone(),
            strategy_data_emitter: strategy_data_emitter.clone(),
            trading_data,
            trading_data_schema,
            // trading_data_update_listener: trading_data_update_listener.clone(),
            // trader_exchange_listener: trader_exchange_listener.clone(),
            // update_balance_listener: update_balance_listener.clone(),
            // update_executions_listener: update_executions_listener.clone(),
            // update_order_listener: update_order_listener.clone(),
        }
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
                    KlinesDataUpdate::Initial(initial_klines_df) => {
                        match data_feed.handle_initial_klines(initial_klines_df) {
                            Ok(initial_strategy_df) => {
                                let payload = StrategyDataUpdate::Initial(initial_strategy_df);
                                data_feed.strategy_data_emitter.next(payload);
                            }
                            Err(error) => {
                                println!("handle_initial_klines error {:?}", error);
                            }
                        }
                    }
                    KlinesDataUpdate::Market(market_klines_df) => {
                        match data_feed.handle_market_klines(market_klines_df) {
                            Ok(updated_strategy_df) => {
                                let payload = StrategyDataUpdate::Market(updated_strategy_df);
                                data_feed.strategy_data_emitter.next(payload);
                            }
                            Err(error) => {
                                println!("handle_market_klines error {:?}", error);
                            }
                        }
                    }
                    _ => {}
                }

                // match trading_data_update {
                //     TradingDataUpdate::InitialKlinesData(initial_klines_lf) => {
                //         data_feed.handle_initial_klines_data(initial_klines_lf)
                //     }
                //     TradingDataUpdate::InitialStrategyData(initial_strategy_lf) => {
                //         match data_feed.compute_benchmark_positions(&initial_strategy_lf) {
                //             Ok(benchmark_lf) => {
                //                 let trading_data_update =
                //                     TradingDataUpdate::BenchmarkData(benchmark_lf);
                //                 data_feed
                //                     .trading_data_update_listener
                //                     .next(trading_data_update);
                //             }
                //             Err(error) => {
                //                 todo!("treat error");
                //             }
                //         }
                //     }
                //     TradingDataUpdate::KlinesData(klines_data) => {
                //         data_feed.handle_klines_data(klines_data)
                //     }
                //     TradingDataUpdate::StrategyData(strategy_lf) => {
                //         // let strategy_data = trading_data_listener.value();
                //         // let exchange_socket_error_ts;
                //         // {
                //         //     let exchange_socket_error_guard = exchange_socket_error_arc.lock().expect(
                //         //         "TradingDataUpdate::StrategyData -> exchange_socket_error_guard.unwrap",
                //         //     );
                //         //     exchange_socket_error_ts = exchange_socket_error_guard.clone();
                //         // }

                //         // // checks for exchange ws error
                //         // if exchange_socket_error_ts.is_some() {
                //         //     // in case of exchange ws error, this function fetches updates at this point and update listener accordingly
                //         //     // TODO: check this
                //         //     // let _ = update_position_data_on_faulty_exchange_ws(
                //         //     //     &exchange_socket_error_arc,
                //         //     //     &exchange_listener,
                //         //     //     &current_trade_listener,
                //         //     //     &update_balance_listener,
                //         //     //     &update_order_listener,
                //         //     //     &update_executions_listener,
                //         //     // )
                //         //     // .await;
                //         // }
                //         match data_feed.update_trading_data(strategy_lf) {
                //             Ok(updated_trading_data) => {
                //                 // TODO: store this as field
                //                 {
                //                     let mut lock = data_feed
                //                         .trading_data
                //                         .lock()
                //                         .expect("trading data deadlock");
                //                     *lock = updated_trading_data.clone();
                //                 }

                //                 let trading_data_update =
                //                     TradingDataUpdate::EmitSignal(updated_trading_data);

                //                 data_feed
                //                     .trading_data_update_listener
                //                     .next(trading_data_update);
                //             }
                //             Err(error) => {
                //                 todo!("treat error");
                //             }
                //         }
                //     }
                //     TradingDataUpdate::EmitSignal(updated_trading_data) => {
                //         // TODO: updated_trading_data can be taken from data_feed = self

                //         match data_feed.generate_last_position_signal(&updated_trading_data) {
                //             Ok(signal) => {
                //                 data_feed.signal_listener.next(Some(signal));
                //                 let trading_data_update =
                //                     TradingDataUpdate::CleanUp(updated_trading_data);

                //                 data_feed
                //                     .trading_data_update_listener
                //                     .next(trading_data_update);
                //             }
                //             Err(error) => {
                //                 todo!("treat error");
                //             }
                //         }
                //     }
                //     TradingDataUpdate::CleanUp(trading_data) => {
                //         // TODD: check if this is supposed to be at trader
                //         // let trading_data = trading_data_listener.value();
                //         // let mut performance_guard = performance_arc
                //         //     .lock()
                //         //     .expect("TradingDataUpdate::CleanUp -> performance_arc.unwrap");
                //         // let _ = performance_guard.update_trading_stats(&trading_data);

                //         // let current_trade = current_trade_listener.value();

                //         // if let Some(current_trade) = current_trade {
                //         //     let mut temp_executions_guard = temp_executions_arc
                //         //         .lock()
                //         //         .expect("TradingDataUpdate::CleanUp -> temp_executions deadlock");

                //         //     let open_order_uuid = &current_trade.open_order.uuid;

                //         //     let close_order_uuid =
                //         //         &current_trade.close_order.clone().unwrap_or_default().uuid;

                //         //     let mut pending_executions = vec![];
                //         //     let mut removed_executions_ids = vec![];

                //         //     while let Some(execution) = temp_executions_guard.iter().next() {
                //         //         if &execution.order_uuid == open_order_uuid
                //         //             || close_order_uuid != ""
                //         //                 && &execution.order_uuid == close_order_uuid
                //         //         {
                //         //             pending_executions.push(execution.clone());
                //         //             removed_executions_ids.push(execution.id.clone());
                //         //         }
                //         //     }

                //         //     if pending_executions.len() > 0 {
                //         //         let updated_trade = current_trade
                //         //             .update_executions(pending_executions)
                //         //             .expect("TradingDataUpdate::CleanUp update_executions unwrap");

                //         //         if updated_trade.is_some() {
                //         //             let updated_trade = updated_trade.unwrap();
                //         //             current_trade_listener.next(Some(updated_trade));

                //         //             let filtered_temp_executions = temp_executions_guard
                //         //                 .clone()
                //         //                 .into_iter()
                //         //                 .filter(|execution| {
                //         //                     !removed_executions_ids.contains(&execution.id)
                //         //                 })
                //         //                 .collect::<Vec<Execution>>();

                //         //             *temp_executions_guard = filtered_temp_executions;
                //         //         }
                //         //     }
                //         // }
                //     }
                //     _ => {}
                // }
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

    fn init_data_provider(&self) -> JoinHandle<()> {
        let mut data_feed = self.clone();
        spawn(async move {
            let _ = data_feed
                .data_provider_exchange
                .init(
                    data_feed.benchmark_datetimes.0,
                    data_feed.benchmark_datetimes.1,
                    data_feed.run_benchmark_only,
                    data_feed.trading_data_schema,
                )
                .await;
        })
    }

    pub async fn init(&'static mut self) -> Result<(), GlowError> {
        self.init_kline_data_handler();
        self.init_data_provider_handler();

        // let _ = data_provider_handle.await;

        // if run_benchmark_only {
        //     return Ok(());
        // }
        // TODO: move to trader?
        // let mut trader_binding = self.trader_exchange_listener.value();
        // let trader_handle = spawn(async move { trader_binding.init().await });
        // let _ = trader_handle.await;

        Ok(())
    }
}

// #[test]
// fn test_vstack() {
//     let df1 = df![
//         "start_time" => &[1, 2, 3],
//         "col2" => &["1", "2", "3"],
//         "col3" => &["1", "2", "3"],
//     ].unwrap();

//     let df2 = df![
//         "start_time" => &[4],
//         "col2" => &["4"],
//     ].unwrap();

//     let df2 = coerce_df_to_schema(df2, &df1.schema()).unwrap();

//     let result_df = df1.vstack(&df2).unwrap();

//     println!("{:?}", result_df);
// }
