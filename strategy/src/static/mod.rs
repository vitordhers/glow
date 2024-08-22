use crate::{
    enums::StrategyId,
    preindicators::{ema::EMA, PreIndicatorWrapper},
    structs::Strategy,
};
use std::{collections::HashMap, sync::LazyLock};

pub static STRATEGIES_MAP: LazyLock<HashMap<StrategyId, Strategy>> = LazyLock::new(|| {
    let mut strategies_map = HashMap::new();

    {
        let name = "Simple Trend Strategy";
        let simple_trend_preindicators = vec![PreIndicatorWrapper::Ema(EMA::default())];
        let simple_trend_indicators = vec![];
        let simple_trend_signals = vec![];
        let simple_trend_strategy = Strategy::new(
            name,
            simple_trend_preindicators,
            simple_trend_indicators,
            simple_trend_signals,
        );
        strategies_map.insert(StrategyId::SimpleTrend, simple_trend_strategy);
    }

    strategies_map
});

pub fn get_default_strategy() -> Strategy {
    let default_strategy = &StrategyId::default();

    STRATEGIES_MAP
        .get(&StrategyId::default())
        .expect(&format!(
            "Default Strategy {:?} to exist at STRATEGIES MAP",
            default_strategy
        ))
        .clone()
}

pub fn get_strategy(id: StrategyId) -> Strategy {
    STRATEGIES_MAP
        .get(&id)
        .expect(&format!(
            "Default Strategy Id{:?} to exist at STRATEGIES MAP",
            id
        ))
        .clone()
}
