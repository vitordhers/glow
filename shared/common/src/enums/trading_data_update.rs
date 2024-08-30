use polars::prelude::{DataFrame, LazyFrame};


/// piping schema: https://app.clickup.com/9013233975/v/wb/8ckp29q-533
#[derive(Clone, Default)]
pub enum TradingDataUpdate {
    #[default]
    None,
    InitialKlinesData(LazyFrame),
    InitialStrategyData(LazyFrame),
    BenchmarkData(LazyFrame), // listens at performance
    KlinesData(DataFrame),
    StrategyData(DataFrame),
    EmitSignal (DataFrame),
    CleanUp(DataFrame),
}
