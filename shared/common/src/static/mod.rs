use crate::{enums::symbol_id::SymbolId, structs::Symbol};
use phf::{self, phf_map, Map};
use std::sync::LazyLock;

pub const DEFAULT_SYMBOL: &'static str = "BTCUSDT";

pub static SYMBOLS_LIST: LazyLock<[&'static str; 5]> =
    LazyLock::new(|| [DEFAULT_SYMBOL, "ETHUSDT", "SOLUSDT", "ARBUSDT", "LINKUSDT"]);

/// Keep this commented out for checking whether there are any performance advantages for using
/// phf_map over HashMap
// use std::collections::HashMap;
// pub static SYMBOLS_MAP: LazyLock<HashMap<&'static str, Symbol>> = LazyLock::new(|| {
//     let mut symbols_map = HashMap::new();
//     for symbol in SYMBOLS_LIST.iter() {
//         symbols_map.insert(*symbol, Symbol::new(symbol));
//     }
//     symbols_map
// });

pub static SYMBOLS_MAP: Map<&'static str, Symbol> = phf_map! {
    "BTCUSDT" => Symbol {id: SymbolId::Bitcoin, name: "BTCUSDT", open: "BTCUSDT_open", high: "BTCUSDT_high", low: "BTCUSDT_low", close: "BTCUSDT_close"},
    "ETHUSDT" => Symbol {id: SymbolId::Ethereum, name: "ETHUSDT", open: "ETHUSDT_open", high: "ETHUSDT_high", low: "ETHUSDT_low", close: "ETHUSDT_close"},
    "SOLUSDT" => Symbol {id: SymbolId::Solana, name: "SOLUSDT", open: "SOLUSDT_open", high: "SOLUSDT_high", low: "SOLUSDT_low", close: "SOLUSDT_close"},
    "ARBUSDT" => Symbol {id: SymbolId::Arbitrum, name: "ARBUSDT", open: "ARBUSDT_open", high: "ARBUSDT_high", low: "ARBUSDT_low", close: "ARBUSDT_close"},
    "LINKUSDT" => Symbol {id: SymbolId::Chainlink, name: "LINKUSDT", open: "LINKUSDT_open", high: "LINKUSDT_high", low: "LINKUSDT_low", close: "LINKUSDT_close"},
};

pub fn get_default_symbol() -> &'static Symbol {
    SYMBOLS_MAP.get(DEFAULT_SYMBOL).unwrap()
}
