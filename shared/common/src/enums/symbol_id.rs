use serde::{Deserialize, Serialize};
use serde_json::to_string;

use crate::{r#static::SYMBOLS_MAP, structs::Symbol};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SymbolId {
    #[default]
    #[serde(rename = "BTCUSDT")]
    Bitcoin,
    #[serde(rename = "ETHUSDT")]
    Ethereum,
    #[serde(rename = "SOLUSDT")]
    Solana,
    #[serde(rename = "ARBUSDT")]
    Arbitrum,
    #[serde(rename = "LINKUSDT")]
    Chainlink,
}

impl SymbolId {
    pub fn get_symbol_str(&self) -> &str {
        match self {
            Self::Bitcoin => "BTCUSDT",
            Self::Ethereum => "ETHUSDT",
            Self::Solana => "SOLUSDT",
            Self::Arbitrum => "ARBUSDT",
            Self::Chainlink => "LINKUSDT",
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
// use glow_error::GlowError;
// fn test_serialization() -> Result<(), GlowError> {
//     let result = to_string(&SymbolId::Chainlink)?;
//     print!("THIS IS RESULT: {}", result);

//     Ok(())
// }
