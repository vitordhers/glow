use super::{Symbol, SymbolsPair};
use crate::enums::{
    granularity::Granularity,
    modifiers::{leverage::Leverage, position_lock::PositionLock, price_level::PriceLevel},
    order_type::OrderType,
    symbol_id::SymbolId,
};
use glow_error::GlowError;
use serde::{Deserialize, Serialize};
use serde_json::{from_reader, to_writer};
use std::{
    collections::HashMap,
    env::{self},
    fmt::{Debug, Formatter, Result as DebugResult},
    fs::File,
    io::{BufReader, Result as IoResult},
};

#[derive(Clone, Serialize, Deserialize)]
// #[serde(bound(deserialize = "'de: 'static"))]
pub struct TradingSettings {
    pub allocation_percentage: f64,
    pub leverage: Leverage,
    pub order_types: (OrderType, OrderType),
    pub position_lock_modifier: PositionLock,
    pub price_level_modifier_map: HashMap<String, PriceLevel>,
    pub signals_revert_its_opposite: bool,
    pub symbols_pair: SymbolsPair,
    pub bechmark_minimum_days: u32,
    pub granularity: Granularity,
}

impl TradingSettings {
    pub fn new(
        allocation_percentage: f64,
        leverage: Leverage,
        open_order_type: OrderType,
        close_order_type: OrderType,
        position_lock_modifier: PositionLock,
        price_level_modifier_map: HashMap<String, PriceLevel>,
        signals_revert_its_opposite: bool,
        anchor_contract_symbol: &SymbolId,
        traded_contract_symbol: &SymbolId,
        bechmark_minimum_days: u32,
        granularity: Granularity,
    ) -> Self {
        TradingSettings {
            allocation_percentage,
            leverage,
            order_types: (open_order_type, close_order_type),
            position_lock_modifier,
            price_level_modifier_map,
            signals_revert_its_opposite,
            symbols_pair: SymbolsPair::new(&anchor_contract_symbol, &traded_contract_symbol),
            bechmark_minimum_days,
            granularity,
        }
    }

    fn get_config_file_path() -> Result<String, GlowError> {
        let args: Vec<String> = env::args().collect();

        match args.get(0) {
            Some(member) => {
                let member = member.split("/").last().unwrap();
                Ok(format!("config/{}/trading_settings.json", member))
            }
            _ => Err(GlowError::new(
                "Invalid -p flag".to_owned(),
                "Invalid -p flag".to_owned(),
            )),
        }
    }

    pub fn load_or_default() -> Self {
        let file_result = File::open(Self::get_config_file_path().unwrap_or_default());
        if let Err(_) = file_result {
            return Self::default();
        }
        let file = file_result.unwrap();
        let reader = BufReader::new(file);
        let loaded_config = from_reader(reader).unwrap_or_default();

        loaded_config
    }

    pub fn save_config(&self) -> IoResult<()> {
        let file = File::create(Self::get_config_file_path().unwrap_or_default())?;
        to_writer(file, self)?;
        Ok(())
    }

    #[inline]
    pub fn get_anchor_symbol(&self) -> &'static Symbol {
        self.symbols_pair.anchor
    }

    #[inline]
    pub fn get_traded_symbol(&self) -> &'static Symbol {
        self.symbols_pair.traded
    }

    pub fn get_unique_symbols(&self) -> Vec<&'static Symbol> {
        self.symbols_pair.get_unique_symbols()
    }

    pub fn get_open_order_type(&self) -> OrderType {
        self.order_types.0
    }

    pub fn get_close_order_type(&self) -> OrderType {
        self.order_types.1
    }

    pub fn patch_symbols_pair(&self, updated_symbols_pair: SymbolsPair) -> Self {
        let mut result = self.clone();
        result.symbols_pair = updated_symbols_pair;

        result
    }

    pub fn fmt_price_level_modifiers(&self) -> String {
        let str = if self.price_level_modifier_map.len() == 0 {
            "No price modifiers".to_owned()
        } else {
            self.price_level_modifier_map.clone().into_iter().fold(
                "".to_owned(),
                |s, (_, price_level)| {
                    if s == "" {
                        format!("{:?}", price_level)
                    } else {
                        format!("{}, {:?}", s, price_level)
                    }
                },
            )
        };
        format!("{}", str)
    }
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            allocation_percentage: 100.0,
            leverage: Leverage::default(),
            order_types: (OrderType::default(), OrderType::default()),
            position_lock_modifier: PositionLock::default(),
            price_level_modifier_map: HashMap::new(),
            signals_revert_its_opposite: false,
            symbols_pair: SymbolsPair::default(),
            granularity: Granularity::default(),
            bechmark_minimum_days: 1,
        }
    }
}

impl Debug for TradingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> DebugResult {
        write!(
            f,
            r#"
            🪙  Symbol pair: {:?}
            🕒 Granularity: {:?}
            💰 Allocation Percentage (%): {:?}
            🎰 Leverage: {:?}
            📒 Order types: Buy {:?}, Sell {:?}
            🎭 Price Modifiers: {:?}
            🔒 Position Lock: {:?}
            🔁 Revert Opposite Signals {}
            📅 Minimum days for benchmarking {}"#,
            self.symbols_pair,
            self.granularity,
            self.allocation_percentage,
            self.leverage,
            self.order_types.0,
            self.order_types.1,
            self.fmt_price_level_modifiers(),
            self.position_lock_modifier,
            self.signals_revert_its_opposite,
            self.bechmark_minimum_days
        )
    }
}
