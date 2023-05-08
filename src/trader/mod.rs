use enums::log_level::LogLevel;
use models::{market_data::MarketDataFeed, performance::Performance, strategy::Strategy};

mod constants;
pub mod enums;
pub mod errors;
mod functions;
pub mod indicators;
pub mod models;
#[allow(dead_code)]
pub struct Trader<'a> {
    symbols: [String; 2],
    units: i64,
    initial_balance: i64,
    current_balance: i64,
    pub market_data_feed: MarketDataFeed<'a>,
    pub performance: Performance,
}

impl<'a> Trader<'a> {
    pub fn new(
        symbols: &'a [String; 2],
        bar_length: i64,
        initial_balance: i64,
        strategy: &'a mut Strategy,
        log_level: LogLevel,
    ) -> Trader<'a> {
        // let strategy_ref = RefCell::new(strategy);
        let market_data_feed = MarketDataFeed::new(&symbols, bar_length, strategy, log_level);
        let performance = Performance::default();

        Trader {
            symbols: symbols.clone(),
            market_data_feed,
            performance,
            initial_balance,
            current_balance: initial_balance,
            units: 0,
        }
    }
}
