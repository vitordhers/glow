use chrono::NaiveDateTime;
use polars::prelude::{DataFrame, LazyFrame};

#[derive(Clone)]
pub enum TradingDataUpdate {
    None,
    BenchmarkData {
        initial_tick_data_lf: LazyFrame,
        initial_last_bar: NaiveDateTime,
    },
    MarketData {
        last_period_tick_data: DataFrame,
    },
    StrategyData {
        strategy_data: DataFrame,
    },
    EmitSignal {
        trading_data: DataFrame,
    },
    CleanUp {
        trading_data: DataFrame,
    },
}
