use crate::enums::{
    order_status::OrderStatus, order_type::OrderType, side::Side, time_in_force::TimeInForce,
};

use super::Execution;

#[derive(Debug, Clone, Default)]
pub struct Order {
    // defined as `{traded_symbol}_{timestamp}_{order_position}
    pub uuid: String,
    pub id: String,
    pub symbol: String,
    pub status: OrderStatus,
    pub order_type: OrderType,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub units: f64,
    pub leverage_factor: f64,
    pub stop_loss_price: Option<f64>,
    pub take_profit_price: Option<f64>,
    pub avg_price: Option<f64>,
    pub taker_fee_rate: f64,
    pub executions: Vec<Execution>,
    pub balance_remainder: f64,
    pub is_stop: bool,
    pub is_close: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Order {
    pub fn new(
        uuid: String,
        id: String,
        symbol: String,
        status: OrderStatus,
        order_type: OrderType,
        side: Side,
        time_in_force: TimeInForce,
        units: f64,
        leverage_factor: f64,
        stop_loss_price: Option<f64>,
        take_profit_price: Option<f64>,
        avg_price: Option<f64>,
        executions: Vec<Execution>,
        taker_fee_rate: f64,
        balance_remainder: f64,
        is_close: bool,
        is_stop: bool,
        created_at: i64,
        updated_at: i64,
    ) -> Self {
        Order {
            uuid,
            id,
            symbol,
            status,
            order_type,
            side,
            time_in_force,
            units,
            leverage_factor,
            stop_loss_price,
            take_profit_price,
            avg_price,
            taker_fee_rate,
            executions,
            balance_remainder,
            is_close,
            is_stop,
            created_at,
            updated_at,
        }
    }

    fn get_order_initial_margin(&self) -> Option<f64> {
        let order_value = self.get_order_value();
        if order_value.is_none() {
            return None;
        }
        let order_value = order_value.unwrap();
        Some(order_value / self.leverage_factor)
    }

    pub fn get_order_cost(&self) -> Option<f64> {
        let initial_margin = self.get_order_initial_margin();
        if initial_margin.is_none() {
            return None;
        }
        let initial_margin = initial_margin.unwrap();

        if self.avg_price.is_none() {
            return None;
        }
        let open_price = self.avg_price.unwrap();

        let bankruptcy_price = self.get_bankruptcy_price();
        if bankruptcy_price.is_none() {
            return None;
        }

        let bankruptcy_price = bankruptcy_price.unwrap();
        let open_position_fee = self.units * open_price * self.taker_fee_rate;
        let close_position_fee = self.units * bankruptcy_price * self.taker_fee_rate;

        Some(initial_margin + open_position_fee + close_position_fee)
    }

    pub fn get_bankruptcy_price(&self) -> Option<f64> {
        if self.side == Side::Buy {
            if self.avg_price.is_none() {
                return None;
            }
            let price = self.avg_price.unwrap();
            Some(price * (self.leverage_factor - 1.0) / self.leverage_factor)
        } else if self.side == Side::Sell {
            if self.avg_price.is_none() {
                return None;
            }
            let price = self.avg_price.unwrap();
            Some(price * (self.leverage_factor + 1.0) / self.leverage_factor)
        } else {
            None
        }
    }

    pub fn is_cancel_order(&self) -> bool {
        match self.status {
            OrderStatus::Cancelled => true,
            _ => false,
        }
    }

    pub fn get_executed_quantity(&self) -> f64 {
        self.executions
            .iter()
            .fold(0.0, |acc, execution| acc + execution.qty)
    }

    pub fn get_executed_avg_price(&self) -> f64 {
        let total_qty = self.get_executed_quantity();

        self.executions.iter().fold(0.0, |acc, execution| {
            acc + (execution.qty * execution.price / total_qty)
        })
    }

    pub fn get_order_value(&self) -> Option<f64> {
        if self.avg_price.is_none() {
            return None;
        }
        let price = self.avg_price.unwrap();
        Some(price * self.units)
    }

    pub fn get_executed_order_value(&self) -> f64 {
        match self.status {
            OrderStatus::Cancelled | OrderStatus::StandBy | OrderStatus::Closed => 0.0,
            _ => self.executions.iter().fold(0.0, |acc, execution| {
                acc + (execution.qty * execution.price)
            }),
        }
    }

    pub fn get_estimate_close_order_fee(&self, fee_rate: f64, est_price: f64) -> f64 {
        self.units * fee_rate * est_price
    }

    pub fn get_executed_order_fee(&self) -> f64 {
        self.executions
            .iter()
            .fold(0.0, |acc, execution| acc + execution.fee)
    }

    pub fn get_closed_quanitity(&self) -> f64 {
        self.executions
            .iter()
            .fold(0.0, |acc, execution| acc + execution.closed_qty)
    }

    pub fn get_executions_between_interval(
        &self,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Vec<Execution> {
        self.executions
            .iter()
            .filter(|execution| {
                start_timestamp <= execution.timestamp && execution.timestamp <= end_timestamp
            })
            .cloned()
            .collect()
    }

    pub fn update(
        &self,
        created_at: i64,
        updated_at: i64,
        status: OrderStatus,
        units: f64,
        avg_price: Option<f64>,
        stop_loss_price: Option<f64>,
        take_profit_price: Option<f64>,
        executions: Vec<Execution>,
    ) -> Order {
        let mut updated_order = self.clone();
        updated_order.created_at = created_at;
        updated_order.updated_at = updated_at;
        updated_order.status = status;
        updated_order.stop_loss_price = stop_loss_price;
        updated_order.take_profit_price = take_profit_price;
        // updated_order.executions.extend(executions);
        updated_order.units = units;
        updated_order.avg_price = avg_price;
        if executions.len() > 0 {
            updated_order = updated_order.push_executions_if_new(executions);
        }
        updated_order.update_status();
        updated_order
    }

    pub fn push_executions_if_new(&self, executions: Vec<Execution>) -> Order {
        let mut updated_order = self.clone();

        executions.into_iter().for_each(|exec| {
            let duplicate_execution = updated_order.executions.iter().find(|e| exec.id == e.id);
            if duplicate_execution.is_none() {
                updated_order.executions.push(exec);
            }
        });
        updated_order.update_status();
        updated_order
    }

    pub fn update_units(&mut self, updated_units: f64) {
        self.units = updated_units;
        self.update_status();
    }

    fn update_status(&mut self) {
        if self.units == 0.0 {
            self.status = OrderStatus::Cancelled
        } else if self.units > 0.0 {
            let open_qty = self.get_executed_quantity();
            if open_qty == 0.0 {
                self.status = OrderStatus::StandBy
            } else {
                if self.is_close {
                    let closed_qty = self.get_closed_quanitity();
                    if self.units == closed_qty {
                        self.status = OrderStatus::Closed
                    } else {
                        self.status = OrderStatus::PartiallyClosed
                    }
                } else {
                    if self.units == open_qty {
                        self.status = OrderStatus::Filled
                    } else {
                        self.status = OrderStatus::PartiallyFilled
                    }
                }
            }
        } else {
            panic!(
                "Order -> validate_status -> order has negative units : {:?}",
                self
            );
        }
    }

    pub fn cancel(&self) -> Self {
        let mut updated_order = self.clone();
        match self.status {
            OrderStatus::StandBy | OrderStatus::Cancelled => {
                updated_order.units = 0.0;
            }
            _ => {
                panic!("cancel qty for an invalid status {:?}", self.status);
            }
        }
        updated_order.update_status();
        updated_order
    }
}
