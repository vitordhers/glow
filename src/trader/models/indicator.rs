use std::collections::HashMap;
use polars::prelude::Expr;

pub struct Indicator {
    pub name: String,
    pub window: u32,
    pub signal: Vec<IndicatorType>,
    pub column_generator: HashMap<String, Expr> // name -> generator expression
}

enum IndicatorType {
    GoLong,
    GoShort,
    RevertLong,
    RevertShort,
    CloseLong,
    CloseShort,
    ClosePosition,
    RevertPosition
}