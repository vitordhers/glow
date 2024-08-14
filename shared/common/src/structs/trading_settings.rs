use super::SymbolsPair;
use crate::enums::{
    granularity::Granularity,
    modifiers::{leverage::Leverage, position_lock::PositionLock, price_level::PriceLevel},
    order_type::OrderType,
};
use serde::{Deserialize, Serialize};
use serde_json::{from_reader, to_writer};
use std::{
    collections::{HashMap, HashSet},
    env::var,
    fs::File,
    io::{BufReader, Result as IoResult},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
// #[serde(bound(deserialize = "'de: 'static"))]
pub struct TradingSettings {
    pub allocation_percentage: f64,
    pub leverage: Leverage,
    pub order_types: (OrderType, OrderType),
    pub position_lock_modifier: PositionLock,
    pub price_level_modifier_map: HashMap<String, PriceLevel>,
    pub signals_revert_its_opposite: bool,
    pub symbols: SymbolsPair,
    pub bechmark_min_days: u32,
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
        anchor_contract_symbol: &'static str,
        traded_contract_symbol: &'static str,
        bechmark_min_days: u32,
        granularity: Granularity,
    ) -> Self {
        TradingSettings {
            allocation_percentage,
            leverage,
            order_types: (open_order_type, close_order_type),
            position_lock_modifier,
            price_level_modifier_map,
            signals_revert_its_opposite,
            symbols: SymbolsPair::new(&anchor_contract_symbol, &traded_contract_symbol),
            bechmark_min_days,
            granularity,
        }
    }

    fn get_config_file_path() -> String {
        match var("CARGO_BIN_NAME").as_deref() {
            Ok(member) => format!("config/{}/trading_settings.json", member),
            _ => unreachable!(),
        }
    }

    pub fn load_or_default() -> Self {
        let file_result = File::open(Self::get_config_file_path());
        if let Err(_) = file_result {
            return Self::default();
        }
        let file = file_result.unwrap();
        let reader = BufReader::new(file);
        let loaded_config = from_reader(reader).unwrap_or_default();

        loaded_config
    }

    pub fn save_config(&self) -> IoResult<()> {
        let file = File::create(Self::get_config_file_path())?;
        to_writer(file, self)?;
        Ok(())
    }

    #[inline]
    pub fn get_anchor_symbol(&self) -> &'static str {
        self.symbols.anchor.name
    }

    #[inline]
    pub fn get_traded_symbol(&self) -> &'static str {
        self.symbols.traded.name
    }

    pub fn get_unique_symbols(&self) -> Vec<&'static str> {
        let mut symbols = HashSet::new();
        symbols.insert(self.get_anchor_symbol());
        symbols.insert(self.get_traded_symbol());
        symbols.into_iter().collect()
    }

    pub fn get_open_order_type(&self) -> OrderType {
        self.order_types.0
    }

    pub fn get_close_order_type(&self) -> OrderType {
        self.order_types.1
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
            symbols: SymbolsPair::default(),
            granularity: Granularity::default(),
            bechmark_min_days: 1,
        }
    }
}
