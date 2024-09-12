use chrono::Duration;
use cli::{change_benchmark_datetimes, change_symbols_pair, select_from_list};
use common::functions::current_datetime;
use common::traits::exchange::TraderHelper;
use tokio::time::sleep;
use core::controller::Controller;
use std::time::Duration as StdDuration;
use dialoguer::console::Term;
use dotenv::dotenv;

use std::env;

#[tokio::main]
async fn main() {
    // todo!()
    dotenv().ok();
    let max_rows = "40".to_string();
    env::set_var("POLARS_FMT_MAX_ROWS", max_rows);

    let mut end_datetime = current_datetime();
    let mut start_datetime = end_datetime - Duration::days(1);

    let term = Term::stdout();
    let mut controller = Controller::new(true);
    loop {
        term.clear_screen().unwrap();

        term.write_line("Glow Backtesting Suite - v0.02.").unwrap();
        term.write_line(
            r#"Gloria Patri, et Filio, et Spiritui Sancto.
            Sicut erat in principio, et nunc et semper, et in saecula saeculorum.
            Amen 🙏"#,
        )
        .unwrap();

        term.write_line(&format!(
            "Current Start Time: {:?} \nCurrent End Time: {:?}\nCurrent Settings {:?}",
            start_datetime,
            end_datetime,
            controller.trader.trader_exchange.get_trading_settings()
        ))
        .unwrap();

        let options = vec![
            "📆 Select Benchmark Datetimes",
            "🪙  Change Trading Coins",
            "📥 Select Data Provider Exchange",
            "📤 Select Trader Exchange",
            "🧐 Change Strategy",
            "🎛  Change Trading Settings",
            "▶️ Run Benchmark",
        ];

        let default_index = options.len() - 1;
        let selection = select_from_list("Select an option", &options, Some(default_index));
        match selection {
            0 => {
                let current_strategy = controller.data_feed.strategy.clone();
                let current_trade_exchange = controller.trader.trader_exchange.clone();
                let result = change_benchmark_datetimes(
                    start_datetime,
                    end_datetime,
                    &current_trade_exchange,
                    current_strategy.get_minimum_klines_for_calculation(),
                );
                if result.is_none() {
                    continue;
                }
                let (updated_start_datetime, updated_end_datetime) = result.unwrap();
                start_datetime = updated_start_datetime;
                end_datetime = updated_end_datetime;
            }
            1 => {
                let current_trading_settings =
                    controller.trader.trader_exchange.get_trading_settings();
                let current_symbols_pair = current_trading_settings.symbols_pair.clone();
                let updated_symbols_pair = change_symbols_pair(current_symbols_pair);
                if updated_symbols_pair.is_none() {
                    continue;
                }
                let updated_symbols_pair = updated_symbols_pair.unwrap();
                let updated_trading_settings =
                    current_trading_settings.patch_symbols_pair(updated_symbols_pair);
                controller.patch_settings(&updated_trading_settings);
            }
            2 => {
                // CHANGE PROVIDER EXCHANGE
            }
            3 => {
                // CHANGE TRADER EXCHANGE
            }
            4 => {
                // CHANGE STRATEGY
            }
            5 => {
                // CHANGE TRADING SETTINGS
            }
            6 => {
                // RUN BENCHMARK
                println!("RUN BENCHMARK SELECTED");
                controller.init();
            }
            selection => {
                println!("Invalid option {}", selection);
                continue;
            }
        }
        let options = vec!["Press enter to run again"];
        select_from_list("Benchmark is done", &options, Some(default_index));
        sleep(StdDuration::new(5, 0)).await;
        


    }
}
