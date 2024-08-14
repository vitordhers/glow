use std::{collections::HashMap, sync::LazyLock};
use crate::{preindicators::PreIndicatorWrapper, Strategy};

pub static STRATEGIES_MAP: LazyLock<HashMap<&'static str, Symbol>> = LazyLock::new(|| {
    let mut strategies_map = HashMap::new();

    PreIndicatorWrapper::Ema(())
    let simple_trend_strategy_preindicators = vec![];
    let simple_trend_strategy = Strategy::new("Simple Trend Strategy", preindicators, indicators, signals)
    strategies_map
});
