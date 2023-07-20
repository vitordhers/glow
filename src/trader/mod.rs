use self::models::signal::SignalCategory;
use self::{errors::Error, models::behavior_subject::BehaviorSubject};
use enums::log_level::LogLevel;
use futures_util::StreamExt;
use models::{market_data::MarketDataFeed, performance::Performance, strategy::Strategy};
use polars::prelude::*;
use std::sync::{Arc, Mutex};

mod constants;
pub mod contracts;
pub mod enums;
pub mod errors;
mod functions;
pub mod indicators;
pub mod models;
pub mod signals;
#[derive(Clone)]
pub struct Trader {
    pub symbols: [String; 2],
    pub units: i64,
    pub initial_balance: f64,
    pub current_balance: f64,
    pub market_data_feed: MarketDataFeed,
    pub strategy_arc: Arc<Mutex<Strategy>>,
    pub performance_arc: Arc<Mutex<Performance>>,
    pub data: BehaviorSubject<DataFrame>,
}

impl Trader {
    pub fn new(
        symbols: &[String; 2],
        initial_balance: f64,
        market_data_feed: MarketDataFeed,
        strategy_arc: Arc<Mutex<Strategy>>,
        performance_arc: Arc<Mutex<Performance>>,
        data: BehaviorSubject<DataFrame>,
        log_level: &LogLevel,
    ) -> Trader {
        Trader {
            symbols: symbols.clone(),
            initial_balance,
            current_balance: initial_balance,
            units: 0,
            market_data_feed: market_data_feed,
            strategy_arc: strategy_arc.clone(),
            performance_arc: performance_arc.clone(),
            data,
        }
    }

    pub async fn init(mut self, signal_subject: BehaviorSubject<Option<SignalCategory>>) {
        let market_data = self.market_data_feed.clone();

        let handle1 = tokio::spawn(async move {
            market_data.fetch_benchmark_data().await;
        });

        let signal_subject_clone = signal_subject.clone();

        let handle2 = tokio::spawn(async move {
            self.market_data_feed.init(signal_subject_clone).await;
        });

        let handle3 = tokio::spawn(async move {
            let mut subscription = signal_subject.subscribe();
            while let Some(signal_opt) = subscription.next().await {
                let mut df = self.data.value();
                df = process_last_signal(signal_opt, df).await.unwrap();
                
            }
        });

        let _ = handle1.await;
        let _ = handle2.await;
        let _ = handle3.await;
    }
}

async fn process_last_signal(
    signal_opt: Option<SignalCategory>,
    df: DataFrame,
) -> Result<DataFrame, Error> {
    match signal_opt {
        Some(signal) => match signal {
            SignalCategory::CloseLong
            | SignalCategory::CloseShort
            | SignalCategory::ClosePosition => Ok(df),
            SignalCategory::GoLong | SignalCategory::GoShort => Ok(df),
            _ => Ok(df),
        },

        None => Ok(df),
    }
}

// async fn subscribe_to_staging_area(strategy_arc: Arc<Mutex<Strategy>>) {
//     // let subscription =
//     let strategy_guard = strategy_arc.lock().await;
//     let subscription = strategy_guard
//         .staging_area
//         .current_order
//         .subscribe(|order| {
//             Box::pin(async move {
//             sleep(Duration::from_secs(1)).await;

//                 println!("@@@@@ UPDATED ORDER: {:?}", order);
//             })
//         });
//     subscription.await;
// }
// async fn stupid_async() {
//     let test_vec = vec![0, 1, 2, 3];
//     let arc = Arc::new(Mutex::new(test_vec));
//     let arc_clone = arc.clone();

//     let handle = tokio::spawn(async move {
//         let mut test_ref = arc_clone.lock().unwrap();
//         let range = 0..1;
//         let result: Vec<i32> = test_ref.drain(range).collect();
//     });

//     handle.await;
// }
