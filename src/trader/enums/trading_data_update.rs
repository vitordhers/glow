use chrono::NaiveDateTime;
use polars::prelude::{DataFrame, LazyFrame};

#[derive(Clone)]
pub enum TradingDataUpdate {
    Nil,
    BenchmarkData {
        initial_tick_data_lf: LazyFrame,
        initial_last_bar: NaiveDateTime,
    },
    MarketData {
        last_period_tick_data: DataFrame,
    },
    StrategyData,
    EmitSignal,
    CleanUp,
}
