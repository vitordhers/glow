use crate::structs::Symbol;
use std::{collections::HashMap, sync::LazyLock};

pub static SYMBOLS_LIST: LazyLock<Vec<&'static str>> =
    LazyLock::new(|| vec!["BTCUSDT", "ETHUSDT", "SOLUSDT", "ARBUSDT", "LINKUSDT"]);

pub static SYMBOLS_MAP: LazyLock<HashMap<&'static str, Symbol>> = LazyLock::new(|| {
    let mut symbols_map = HashMap::new();
    for symbol in SYMBOLS_LIST.iter().cloned() {
        symbols_map.insert(symbol, Symbol::new(symbol));
    }
    symbols_map
});
