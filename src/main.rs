// use tokio::*;
extern crate dotenv;
use polars::prelude::*;
use std::collections::HashMap;

use dotenv::dotenv;
mod trader;
use log::*;
use trader::{
    enums::log_level::LogLevel,
    indicators::stochastic,
    models::{
        indicator::{Indicator, IndicatorType},
        strategy::Strategy,
    },
    errors::Error,
    Trader,
};

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();
    let anchor_symbol = String::from("BTCUSDT");
    let symbol = String::from("AGIXUSDT");
    let symbols = [anchor_symbol, symbol];
    let mut column_generator: HashMap<
        String,
        Box<dyn Fn(&Indicator, &String, &String, &LazyFrame) -> Result<LazyFrame, Error>>,
    > = HashMap::new();
    column_generator.insert("stochastic_3".into(), Box::new(stochastic));
    let indicators = vec![Indicator::new(
        "stochastic_3".to_string(),
        3,
        vec![IndicatorType::GoShort],
        column_generator,
    )];
    let mut strategy = Strategy::new("Stochastic Strategy".into(), indicators);
    let mut trader = Trader::new(&symbols, 60, 0, &mut strategy, LogLevel::All);

    let connection = trader.market_data_feed.connect().await;

    match connection {
        Err(e) => {
            error!("error found {:?}", e);
        }
        Ok(()) => {
            let _ = trader.market_data_feed.subscribe().await;
            let _events = trader.market_data_feed.listen_events().await;
        }
    }
}
