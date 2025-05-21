use crate::{enums::symbol_id::SymbolId, structs::Symbol};
use phf::{phf_map, Map};
use std::sync::LazyLock;

pub const DEFAULT_SYMBOL: &str = "BTCUSDT";

pub static SYMBOLS_LIST: LazyLock<[&'static str; 5]> =
    LazyLock::new(|| [DEFAULT_SYMBOL, "ETHUSDT", "SOLUSDT", "ARBUSDT", "LINKUSDT"]);

pub static BASES_MAP: LazyLock<[&'static str; 5]> = LazyLock::new(||["BTC", "ETH", "SOL", "ARB", "LINK"]);
pub static QUOTES_MAP: LazyLock<[&'static str; 1]> = LazyLock::new(|| ["USDT"]);

pub static SYMBOLS_MAP: Map<&'static str, Symbol> = phf_map! {
    "BTCUSDT" => Symbol {id: SymbolId::BitcoinUsdt, name: "BTCUSDT", open: "BTCUSDT_open", high: "BTCUSDT_high", low: "BTCUSDT_low", close: "BTCUSDT_close"},
    "ETHUSDT" => Symbol {id: SymbolId::EthereumUsdt, name: "ETHUSDT", open: "ETHUSDT_open", high: "ETHUSDT_high", low: "ETHUSDT_low", close: "ETHUSDT_close"},
    "SOLUSDT" => Symbol {id: SymbolId::SolanaUsdt, name: "SOLUSDT", open: "SOLUSDT_open", high: "SOLUSDT_high", low: "SOLUSDT_low", close: "SOLUSDT_close"},
    "ARBUSDT" => Symbol {id: SymbolId::ArbitrumUsdt, name: "ARBUSDT", open: "ARBUSDT_open", high: "ARBUSDT_high", low: "ARBUSDT_low", close: "ARBUSDT_close"},
    "LINKUSDT" => Symbol {id: SymbolId::LinkUsdt, name: "LINKUSDT", open: "LINKUSDT_open", high: "LINKUSDT_high", low: "LINKUSDT_low", close: "LINKUSDT_close"},
};

pub fn get_default_symbol() -> &'static Symbol {
    SYMBOLS_MAP.get(DEFAULT_SYMBOL).unwrap()
}
