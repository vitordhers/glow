use crate::trader::enums::{
    order_status::OrderStatus, order_type::OrderType, time_in_force::TimeInForce,
};

use super::execution::Execution;

#[derive(Debug, Clone, Default)]
pub struct Order {
    // defined as `{traded_symbol}_{timestamp}_{order_position}
    pub uuid: String,
    pub id: String,
    pub symbol: String,
    pub status: OrderStatus,
    pub is_close: bool,
    pub is_stop: bool,
    pub order_type: OrderType,
    pub units: f64,
    pub avg_price: Option<f64>,
    pub position: i32,
    pub executions: Vec<Execution>,
    pub stop_loss_price: Option<f64>,
    pub take_profit_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Order {
    pub fn new(
        uuid: String,
        id: String,
        symbol: String,
        status: OrderStatus,
        is_close: bool,
        is_stop: bool,
        order_type: OrderType,
        position: i32,
        units: f64,
        avg_price: Option<f64>,
        executions: Vec<Execution>,
        stop_loss_price: Option<f64>,
        take_profit_price: Option<f64>,
        time_in_force: TimeInForce,
        created_at: i64,
        updated_at: i64,
    ) -> Self {
        Order {
            uuid,
            id,
            symbol,
            status,
            is_close,
            is_stop,
            order_type,
            units,
            avg_price,
            position,
            executions,
            stop_loss_price,
            take_profit_price,
            time_in_force,
            created_at,
            updated_at,
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

    pub fn get_executed_order_value(&self) -> f64 {
        match self.status {
            OrderStatus::Cancelled | OrderStatus::StandBy | OrderStatus::Closed => 0.0,
            _ => self.executions.iter().fold(0.0, |acc, execution| {
                acc + (execution.qty * execution.price)
            }),
        }
        // self.executions.iter().fold(0.0, |acc, execution| {
        //     acc + (execution.qty * execution.price)
        // })
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
