use std::collections::HashMap;

use crate::trader::enums::{
    modifiers::{leverage::Leverage, position_lock::PositionLock, price_level::PriceLevel},
    order_type::OrderType,
};

#[derive(Debug, Clone)]
pub struct TradingSettings {
    pub open_order_type: OrderType,
    pub close_order_type: OrderType,
    pub leverage: Leverage,
    pub position_lock_modifier: PositionLock,
    pub price_level_modifier_map: HashMap<String, PriceLevel>,
    pub allocation_percentage: f64,
}

impl TradingSettings {
    pub fn new(
        open_order_type: OrderType,
        close_order_type: OrderType,
        leverage: Leverage,
        position_lock_modifier: PositionLock,
        price_level_modifier_map: HashMap<String, PriceLevel>,
        allocation_percentage: f64,
    ) -> Self {
        TradingSettings {
            open_order_type,
            close_order_type,
            leverage,
            position_lock_modifier,
            price_level_modifier_map,
            allocation_percentage,
        }
    }
}
