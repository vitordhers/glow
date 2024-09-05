use polars::prelude::DataFrame;

/// piping schema: https://app.clickup.com/9013233975/v/wb/8ckp29q-533
#[derive(Clone)]
pub enum TradingDataUpdate {
    None,
    Initial(DataFrame),
    Market(DataFrame),
}
