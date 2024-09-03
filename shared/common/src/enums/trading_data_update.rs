use polars::prelude::{DataFrame, LazyFrame};

/// piping schema: https://app.clickup.com/9013233975/v/wb/8ckp29q-533
#[derive(Clone)]
pub enum KlinesDataUpdate {
    None,
    Initial(DataFrame),
    // InitialStrategyData(LazyFrame),
    // BenchmarkData(LazyFrame), // listens at performance
    Market(DataFrame),
    // StrategyData(DataFrame),
    // EmitSignal (DataFrame),
    // CleanUp(DataFrame),
}

#[derive(Clone)]
pub enum StrategyDataUpdate {
    None,
    Initial(DataFrame),
    Market(DataFrame),
}
