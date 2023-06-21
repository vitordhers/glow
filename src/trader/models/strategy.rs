use super::{
    super::constants::SECONDS_IN_DAY,
    super::errors::Error,
    async_behavior_subject::AsyncBehaviorSubject,
    // super::functions::print_as_df,
    indicator::Indicator,
    performance::Performance,
    // market_data::MarketDataFeedDTE,
    signal::{SignalCategory, Signer},
};
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::collections::HashSet;
use std::time::Instant;

// use std::borrow::BorrowMut;
// use async_trait::async_trait;
// use tokio::sync::watch::error::SendError;

// #[derive(Clone)]
pub struct Strategy {
    pub name: String,
    pub pre_indicator_cols: Option<Box<dyn Indicator + Send + Sync>>,
    pub indicators: Vec<Box<dyn Indicator + Send + Sync>>,
    pub signals: Vec<Box<dyn Signer + Send + Sync>>,
    pub data: AsyncBehaviorSubject<LazyFrame>,
    // pub receivers: Vec<StrategyReceiver>,
    // pub sender: Sender<StrategyDTE>,
}
impl Clone for Strategy {
    fn clone(&self) -> Self {
        Self::new(
            self.name.clone(),
            self.pre_indicator_cols.clone(),
            self.indicators.clone(),
            self.signals.clone(),
        )
    }
}
// #[derive(Clone)]
// pub enum StrategyReceiver {
//     MarketDataFeed(Receiver<MarketDataFeedDTE>),
// }

impl Strategy {
    pub fn new(
        name: String,
        pre_indicator_cols: Option<Box<dyn Indicator + Send + Sync>>,
        indicators: Vec<Box<dyn Indicator + Send + Sync>>,
        signals: Vec<Box<dyn Signer + Send + Sync>>,
        // sender: Sender<StrategyDTE>,
        // market_data_feed_receiver: Receiver<MarketDataFeedDTE>,
    ) -> Strategy {
        Strategy {
            name,
            pre_indicator_cols,
            indicators,
            signals,
            data: AsyncBehaviorSubject::new(LazyFrame::default()),
            // sender,
            // receivers: vec![StrategyReceiver::MarketDataFeed(market_data_feed_receiver)],
        }
    }

    // pub async fn listen_events(self) -> Result<Result<(), Error>, JoinError> {
    //     let observed = self.data_feed_arc;
    //     let handle = tokio::spawn(async move {
    //         let lf = wait_for_update(&*observed).await;
    //         let d = NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
    //         let t = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap();
    //         self.set_benchmark(lf.0, &NaiveDateTime::new(d, t), &"AGIX_USDT".to_string())
    //     });
    //     handle.await
    // }

    pub fn set_benchmark(
        &mut self,
        performance: &mut Performance,
        tick_data: LazyFrame,
        last_bar: &NaiveDateTime,
        traded_symbol: &String,
        initial_balance: f64,
    ) -> Result<(), Error> {
        println!("SET BENCHMARK");
        let tick_data = tick_data.cache();
        let mut lf = self.set_benchmark_data(tick_data)?;

        let benchmark_positions = self.compute_benchmark_positions(&lf, true)?;
        lf = lf.left_join(benchmark_positions, "start_time", "start_time");

        // get only signals from previous day
        let minus_one_day_timestamp = last_bar.timestamp() * 1000 - SECONDS_IN_DAY * 1000;

        lf = lf.filter(
            col("start_time")
                .dt()
                .timestamp(TimeUnit::Milliseconds)
                .gt_eq(minus_one_day_timestamp),
        );

        // let path = "data/test".to_string();
        // let file_name = "BTC_USDT_AGIX_USDT.csv".to_string();

        // save_csv(path, file_name, &self.data.clone().collect()?, true)?;

        self.data.next(lf.clone());
        performance.set_benchmark(&lf, traded_symbol, initial_balance)?;

        Ok(())
    }

    pub fn set_benchmark_data(&self, tick_data: LazyFrame) -> Result<LazyFrame, Error> {
        let mut lf = self.set_benchmark_pre_indicators(tick_data)?;
        lf = self.set_benchmark_indicators(&lf)?;
        lf = self.set_signals(&lf)?;
        Ok(lf)
    }

    fn set_benchmark_pre_indicators(&self, tick_data: LazyFrame) -> Result<LazyFrame, Error> {
        match &self.pre_indicator_cols {
            Some(boxed_fn) => {
                let preindicator = boxed_fn.as_ref();
                // print_names(&tick_data, "PRE INDICATORS TICK DATA".into())?;
                let new_data = preindicator.compute_indicator_columns(tick_data.clone())?;
                // print_names(&new_data, "PRE INDICATORS NEW DATA".into())?;

                let lf = tick_data.left_join(new_data, "start_time", "start_time");
                // print_names(&lf, "PRE INDICATORS LF DATA".into())?;

                Ok(lf)
            }
            None => Ok(tick_data),
        }
    }

    fn set_benchmark_indicators(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();

        for indicator in &self.indicators {
            let lf = indicator.compute_indicator_columns(data.clone())?;
            data = data.left_join(lf, "start_time", "start_time");
        }

        Ok(data)
    }

    fn set_signals(&self, data: &LazyFrame) -> Result<LazyFrame, Error> {
        let mut data = data.to_owned();
        for signal in &self.signals {
            let lf = signal.compute_signal_column(&data)?;
            data = data.left_join(lf, "start_time", "start_time");
        }

        Ok(data)
    }

    fn compute_benchmark_positions(
        &self,
        data: &LazyFrame,
        is_benchmark: bool,
    ) -> Result<LazyFrame, Error> {
        // THIS NEEDS OPTIMIZATION, IF POSSIBLE
        let start_time = Instant::now();

        let data = data.to_owned();
        // if is_benchmark {
        //     data = data.with_column(lit(0).alias("position"));
        // }

        // let signal_cols = self.get_signals_categories_cols();

        // let or_cols_expr: Expr = signal_cols
        //     .iter()
        //     .fold(None, |prev: Option<Expr>, curr: &String| {
        //         if let Some(e) = prev {
        //             return Some(e.or(col(curr)));
        //         }
        //         Some(col(curr))
        //     })
        //     .unwrap();

        // let clauses = map_cols_to_clauses(&signal_cols);

        // let columns_clause: Expr = clauses
        //     .iter()
        //     .fold(None, |acc: Option<Expr>, value| {
        //         if let Some(clause) = acc {
        //             return Some(
        //                 when(value.condition.clone())
        //                     .then(value.value.clone())
        //                     .otherwise(clause),
        //             );
        //         }

        //         Some(
        //             when(value.condition.clone())
        //                 .then(value.value.clone())
        //                 .otherwise(col("position").shift(1)),
        //         )
        //     })
        //     .unwrap();

        // let columns_clause_label: Expr = clauses
        //     .iter()
        //     .fold(None, |acc: Option<Expr>, value| {
        //         if let Some(clause) = acc {
        //             return Some(
        //                 when(value.condition.clone())
        //                     .then(value.signal.clone())
        //                     .otherwise(clause),
        //             );
        //         }

        //         Some(
        //             when(value.condition.clone())
        //                 .then(value.signal.clone())
        //                 .otherwise(col("event").shift(1)),
        //         )
        //     })
        //     .unwrap();

        // println!("@@@@@@ {:?}", columns_clause_label);
        let mut df = data.clone().collect()?.drop_nulls::<String>(None)?;
        // let mut iters = df.columns(signal_cols)?.iter().for_each(|s| {
        //     println!("@@@ SERIES {:?}", s);
        // });

        println!("SIZE {:?}", df.estimated_size());

        let mut signals_categories = HashSet::new();

        for signal in self.signals.clone().into_iter() {
            signals_categories.insert(signal.signal_category());
        }

        let contains_long_close = signals_categories.contains(&SignalCategory::CloseLong);
        let contains_short_close = signals_categories.contains(&SignalCategory::CloseShort);
        let contains_position_close = signals_categories.contains(&SignalCategory::ClosePosition);

        let mut positions = vec![];

        for index in 0..df.height() {
            if index == 0 {
                positions.push(0);
                continue;
            }
            let current_position = positions[index - 1];
            let mut updated_position = current_position;

            if current_position == 0 {
                // TODO: GET df["short"] dynamically
                let short = df["short"].get(index)?.eq(&AnyValue::Boolean(true));
                // TODO: GET df["short"] dynamically
                let long = df["long"].get(index)?.eq(&AnyValue::Boolean(true));
                if short == true {
                    updated_position = -1;
                } else if long == true {
                    updated_position = 1;
                }
            } else if current_position == 1 {
                let mut long_close = false;
                if contains_long_close {
                    long_close = df["long_close"].get(index)?.eq(&AnyValue::Boolean(true));
                }
                if contains_position_close {
                    let position_close = df["position_close"]
                        .get(index)?
                        .eq(&AnyValue::Boolean(true));
                    long_close = long_close || position_close;
                }
                if long_close {
                    updated_position = 0;
                }
            } else if current_position == -1 {
                let mut short_close = false;
                if contains_short_close {
                    short_close = df["short_close"].get(index)?.eq(&AnyValue::Boolean(true));
                }
                if contains_position_close {
                    let position_close = df["position_close"]
                        .get(index)?
                        .eq(&AnyValue::Boolean(true));
                    short_close = short_close || position_close;
                }
                // TODO: implement checks for SignalCategory::ClosePosition
                if short_close {
                    updated_position = 0;
                }
            }

            positions.push(updated_position);
        }

        let position_series = Series::new("position", positions);

        let df_with_positions = df.with_column(position_series)?;

        // for row in 0..df.height() {
        //     for iter in &mut iters {
        //         let value = iter.next().expect("should have as many iterations as rows");
        //         println!("@@@ VALUE {:?}", value);
        //     }
        // }

        // data = data
        //     .collect()?
        //     .with_row_count("id", None)?
        //     .lazy()
        //     .with_column(columns_clause.alias("position"))
        //     .with_column(col("id").shift(1).alias("shift_id"))
        //     .with_column(col("position").shift(1).alias("shift_position"))
        //     .select([
        //         col("id"),
        //         col("start_time"),
        //         col("position"),
        //         col("shift_id"),
        //         col("shift_position"),
        //     ]);
        let result = df_with_positions
            .clone()
            .lazy()
            .select([col("start_time"), col("position")]);
        let elapsed_time = start_time.elapsed();
        let elapsed_millis = elapsed_time.as_millis();
        println!("Elapsed time in milliseconds: {}", elapsed_millis);
        Ok(result)
    }

    fn get_signals_categories_cols(&self) -> Vec<String> {
        let mut categories_set: HashSet<String> = HashSet::new();

        for signal in &self.signals {
            let category = signal.signal_category();
            let cat = match category {
                SignalCategory::GoShort => String::from("short"),
                SignalCategory::GoLong => String::from("long"),
                SignalCategory::CloseShort => String::from("short_close"),
                SignalCategory::CloseLong => String::from("long_close"),
                SignalCategory::ClosePosition => String::from("position_close"),
                SignalCategory::RevertShort => String::from("short_revert"),
                SignalCategory::RevertLong => String::from("long_revert"),
                SignalCategory::RevertPosition => String::from("position_revert"),
                SignalCategory::KeepPosition => String::from("position_keep"),
            };
            categories_set.insert(cat);
        }
        categories_set.into_iter().collect()
    }

    #[allow(dead_code)]
    pub fn update_positions(&mut self, _tick_data: &LazyFrame) -> Result<(), Error> {
        Ok(())
    }
}

impl Strategy {
    // pub async fn listen_events(mut self) -> Result<Result<(), Error>, JoinError> {
    //     // let observed_cloned = self.data_feed_arc.clone();

    //     // let handle = tokio::spawn(async move {
    //     //     let observed = observed_cloned;
    //     //     let result = wait_for_update(&*observed).await;
    //     //     let dto = result.0.clone();

    //     //     self.set_benchmark(dto.lf, &dto.last_bar, &dto.traded_symbol)
    //     // });
    //     // handle.await
    //     Ok(())
    // }
}

// impl Default for Strategy<'_> {
//     fn default() -> Self {
//         Strategy {
//             name: "Default Strategy".to_string(),
//             pre_indicator_cols: None,
//             indicators: vec![],
//             signals: vec![],
//             data_feed_arc: Arc::new(MarketDataFeed::default()),
//             // performance: Performance::default(),
//             data: LazyFrame::default(),
//         }
//     }
// }

// #[async_trait]
// impl Subscriber<StrategyReceiver> for Strategy {
//     async fn subscribe(&mut self) -> Result<(), SendError<StrategyReceiver>> {
//         for receiver in self.receivers.clone() {
//             match receiver {
//                 StrategyReceiver::MarketDataFeed(mut rx) => match rx.borrow_mut().changed().await {
//                     Ok(()) => {
//                         let dte = rx.borrow().clone();
//                         match dte {
//                             MarketDataFeedDTE::SetBenchmark(lf, last_bar, traded_symbol) => {
//                                 let _ = self.set_benchmark(lf, &last_bar, &traded_symbol);
//                             }
//                             _ => {}
//                         }
//                     }
//                     Err(err) => println!("STRATEGY SUBSCRIBE ERROR {:?}", err),
//                 },
//             }
//         }

//         Ok(())
//     }
// }

// pub enum StrategyDTE {
//     Nil,
//     SetBenchmark(LazyFrame, NaiveDateTime, String),
// }

// impl Default for StrategyDTE {
//     fn default() -> Self {
//         StrategyDTE::Nil
//     }
// }

fn map_cols_to_clauses(cols: &Vec<String>) -> Vec<ColClause> {
    let mut clauses: Vec<ColClause> = vec![];
    if cols.contains(&String::from("short")) {
        let col_clause = ColClause::new(
            col("short").and(col("position").shift(1).eq(0)),
            lit(-1),
            "short".to_string(),
        );
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("long")) {
        let col_clause = ColClause::new(
            col("long").and(col("position").shift(1).eq(0)),
            lit(1),
            "long".to_string(),
        );
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("short_close")) {
        let condition = col("short_close").and(col("position").shift(1).eq(-1));
        let col_clause = ColClause::new(condition, lit(0), "short_close".to_string());
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("long_close")) {
        let condition = col("long_close").and(col("position").shift(1).eq(1));
        let col_clause = ColClause::new(condition, lit(0), "long_close".to_string());
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("short_reverse")) {
        let condition = col("short_reverse").and(col("position").shift(1).eq(-1));
        let col_clause = ColClause::new(condition, lit(1), "short_reverse".to_string());
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("position_close")) {
        let condition = col("position_close").and(
            col("position")
                .shift(1)
                .eq(1)
                .or(col("position").shift(1).eq(-1)),
        );
        let col_clause = ColClause::new(condition, lit(0), "position_close".to_string());
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("position_revert")) {
        let condition = col("position_close").and(
            col("position")
                .shift(1)
                .eq(1)
                .or(col("position").shift(1).eq(-1)),
        );
        let col_clause = ColClause::new(
            condition,
            col("position").shift(1) * lit(-1),
            "position_revert".to_string(),
        );
        clauses.push(col_clause);
    }
    if cols.contains(&String::from("position_keep")) {
        let condition = col("position_keep");
        let col_clause = ColClause::new(
            condition,
            col("position").shift(1),
            "position_keep".to_string(),
        );
        clauses.push(col_clause);
    }
    clauses
}

struct ColClause {
    pub condition: Expr,
    pub value: Expr,
    pub signal: Expr,
}

impl ColClause {
    pub fn new(condition: Expr, value: Expr, signal: String) -> Self {
        Self {
            condition,
            value,
            signal: lit(signal),
        }
    }
}
