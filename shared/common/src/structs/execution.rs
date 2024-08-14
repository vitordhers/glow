use crate::enums::order_type::OrderType;

#[derive(Debug, Clone)]
pub struct Execution {
    pub id: String,
    pub order_uuid: String,
    pub order_type: OrderType,
    pub timestamp: i64,
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_rate: f64,
    pub is_maker: bool,
    pub closed_qty: f64,
}

impl Execution {
    pub fn new(
        id: String,
        order_uuid: String,
        order_type: OrderType,
        timestamp: i64,
        price: f64,
        qty: f64,
        fee: f64,
        fee_rate: f64,
        is_maker: bool,
        closed_qty: f64,
    ) -> Self {
        Execution {
            id,
            order_uuid,
            order_type,
            timestamp,
            price,
            qty,
            fee,
            fee_rate,
            is_maker,
            closed_qty,
        }
    }

    fn get_value(&self) -> f64 {
        self.qty * self.price
    }
}
