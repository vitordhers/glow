use serde::{Deserialize, Serialize};

use crate::{r#static::SYMBOLS_MAP, structs::Symbol};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SymbolId {
    #[default]
    #[serde(rename = "BTCUSDT")]
    BitcoinUsdt,
    #[serde(rename = "ETHUSDT")]
    EthereumUsdt,
    #[serde(rename = "SOLUSDT")]
    SolanaUsdt,
    #[serde(rename = "ARBUSDT")]
    ArbitrumUsdt,
    #[serde(rename = "LINKUSDT")]
    LinkUsdt,
}

impl SymbolId {
    pub fn get_symbol_str(&self) -> &str {
        match self {
            Self::BitcoinUsdt => "BTCUSDT",
            Self::EthereumUsdt => "ETHUSDT",
            Self::SolanaUsdt => "SOLUSDT",
            Self::ArbitrumUsdt => "ARBUSDT",
            Self::LinkUsdt => "LINKUSDT",
        }
    }

    pub fn get_symbol(&self) -> &Symbol {
        let id_str = self.get_symbol_str();

        SYMBOLS_MAP
            .get(&id_str)
            .expect(&format!("Symbol {} to exist at SYMBOLS_MAP", id_str))
    }
}

// #[test]
// use serde_json::to_string;
// use glow_error::GlowError;
// fn test_serialization() -> Result<(), GlowError> {
//     let result = to_string(&SymbolId::Chainlink)?;
//     print!("THIS IS RESULT: {}", result);

//     Ok(())
// }
