// use tokio::*;
extern crate dotenv;
use dotenv::dotenv;
mod trader;
use trader::{
    Trader,
    enums::log_level::LogLevel
};
use log::*;


#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv().ok();
    let anchor_symbol = String::from("BTCUSDT");
    let symbol = String::from("AGIXUSDT");
    let mut trader = Trader::new(
        symbol,
        anchor_symbol,
        60,
        [5, 10, 15],
        LogLevel::All,
    );

    let connection = trader.connect().await;

    match connection {
        Err(e) => {
            error!("error found {:?}", e);
        },
        Ok(()) => {
            let _ = trader.subscribe().await;
            let _events = trader.listen_events().await;
        }
    }
}
